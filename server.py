import asyncio
import socket
import traceback
from collections import defaultdict
import random
import string
import subprocess
import os
import json

CONTROL_PORT = 7000
PROXY_PORT_RANGE = (8000, 9000)

active_tunnels = {}
data_connections = defaultdict(lambda: defaultdict(asyncio.Queue))

SUBDOMAINS_FILE = "user_subdomains.json"
user_subdomains = {}

subdomain_to_rp = {}

server = None

# ==================================
# 1. ФУНКЦИИ ЧТЕНИЯ / ЗАПИСИ SUBDOMAIN
# ==================================
def save_subdomains():
    """Сохраняет словарь user_subdomains в JSON файл"""
    try:
        with open(SUBDOMAINS_FILE, 'w') as f:
            json.dump(user_subdomains, f, indent=4)
        print(f"[STORAGE] Сохранено {len(user_subdomains)} поддоменов в user_subdomains.json")
    except Exception as e:
        print(f"[STORAGE] Ошибка при сохранении поддоменов: {e}")

def load_subdomains():
    """Загружает словарь user_subdomains из JSON файла"""
    global user_subdomains
    try:
        if os.path.exists(SUBDOMAINS_FILE):
            with open(SUBDOMAINS_FILE, 'r') as f:
                user_subdomains = json.load(f)
            print(f"[STORAGE] Загружено {len(user_subdomains)} поддоменов из user_subdomains.json")
        else:
            print("[STORAGE] Файл с поддоменами не найден, используем пустой словарь")
    except Exception as e:
        print(f"[STORAGE] Ошибка при загрузке поддоменов: {e}")
        user_subdomains = {}

# ==================================
# 2. ЗАПУСК ОСНОВНОГО СЕРВЕРА
# ==================================
async def main():
    global server
    
    load_subdomains()
    
    server = await asyncio.start_server(handle_control_or_data_connection, '0.0.0.0', CONTROL_PORT)
    print(f"[MAIN] Listening on {CONTROL_PORT}")
    print("[MAIN] Для выхода введите 'exit'")

    # Задача для PING (чтобы проверять живы ли туннели)
    ping_task = asyncio.create_task(ping_tunnels())

    # Задача для чтения консоли
    console_task = asyncio.create_task(read_console())

    # Запускаем «поддоменный» прокси на Minecraft-порту 25565
    subdomain_proxy_task = asyncio.create_task(start_subdomain_proxy(25565))

    try:
        async with server:
            await server.serve_forever()
    finally:
        ping_task.cancel()
        console_task.cancel()
        subdomain_proxy_task.cancel()
        await asyncio.gather(ping_task, console_task, subdomain_proxy_task, return_exceptions=True)

# ==================================
# 3. ОБРАБОТКА ПОДКЛЮЧЕНИЙ (DATA / CONTROL)
# ==================================
async def handle_control_or_data_connection(reader, writer):
    try:
        line = await reader.readline()
        if not line:
            writer.close()
            await writer.wait_closed()
            return
        msg = line.decode('utf-8-sig').strip()
        parts = msg.split()
        if not parts:
            return

        if parts[0] == "DATA_CONNECTION":
            # Ожидаем 3 части: DATA_CONNECTION <rp> <clientId>
            if len(parts) != 3:
                print("[DATA_CONNECTION] Invalid format:", msg)
                return
            rp = int(parts[1])
            clientId = parts[2]
            if rp not in active_tunnels:
                print(f"[DATA_CONNECTION] Unknown rp={rp}")
                return
            data_connections[rp][clientId].put_nowait((reader, writer))
            qsize = data_connections[rp][clientId].qsize()
            print(f"[DATA] data_connections[{rp}][{clientId}].qsize = {qsize}")
        else:
            await process_control_command(msg, reader, writer)
    except Exception as e:
        print("[handle_control] Error:", e)
        traceback.print_exc()

async def process_control_command(msg, reader, writer):
    parts = msg.split()
    
    if parts[0] == "RESET_SUBDOMAIN":

        clientId = parts[1]
        if clientId in user_subdomains:
            old_sub = user_subdomains[clientId]
            del user_subdomains[clientId]
            rp_for_del = None
            for rp, tunnel_info in active_tunnels.items():
                (cr, cw, srv, cid, sdomain) = tunnel_info
                if cid == clientId and sdomain == old_sub:
                    rp_for_del = rp
                    break
            if rp_for_del:

                await close_tunnel(rp_for_del)

            if old_sub in subdomain_to_rp:
                del subdomain_to_rp[old_sub]

            writer.write(b"SUBDOMAIN_RESET_OK\n")
            print(f"[SUBDOMAIN] Reset subdomain for clientId={clientId}")
        else:
            writer.write(b"SUBDOMAIN_RESET_OK\n")
            print(f"[SUBDOMAIN] No subdomain found for clientId={clientId}")
        
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        return
    
    elif parts[0] == "REQUEST_TUNNEL":
        # REQUEST_TUNNEL <local_port> <clientId> <requested_subdomain>
        local_port = int(parts[1])
        clientId = parts[2]
        subdomain = parts[3]

        rp = find_free_port(PROXY_PORT_RANGE)
        if rp is None:
            writer.write(b"ERROR No free ports\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return

        success, srv = await start_proxy_server(rp)
        if not success:
            writer.write(b"ERROR Cannot start proxy\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return
        
        user_subdomains[clientId] = subdomain
        subdomain_to_rp[subdomain] = rp

        active_tunnels[rp] = (reader, writer, srv, clientId, subdomain)

        resp = f"OK_TUNNEL_CREATED {rp}\n"
        writer.write(resp.encode())
        await writer.drain()
        print(f"[CONTROL] Created tunnel for subdomain={subdomain}, rp={rp}, clientId={clientId}")

    elif parts[0] == "CHECK_SUBDOMAIN":
        # CHECK_SUBDOMAIN <clientId> <requested_subdomain>
        clientId = parts[1]
        requested_subdomain = parts[2]

        # Проверим, не занят ли уже этот поддомен кем-то другим
        if any(sd == requested_subdomain for uid, sd in user_subdomains.items() if uid != clientId):
            writer.write(b"SUBDOMAIN_TAKEN\n")
            print(f"[SUBDOMAIN] Subdomain {requested_subdomain} is taken")
        else:
            # Привязываем клиенту
            user_subdomains[clientId] = requested_subdomain
            writer.write(b"SUBDOMAIN_AVAILABLE\n")
            print(f"[SUBDOMAIN] Assigned subdomain {requested_subdomain} to clientId={clientId}")
        
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        return

    elif parts[0] == "STOP_TUNNEL":
        # STOP_TUNNEL <clientId>
        clientId = parts[1]
        # Ищем туннели клиента
        tunnels_to_close = [(rp, tunnel) for rp, tunnel in active_tunnels.items() 
                            if tunnel[3] == clientId]
        
        if tunnels_to_close:
            for rp, tunnel in tunnels_to_close:
                await close_tunnel(rp)
            writer.write(b"TUNNEL_STOPPED_OK\n")
            print(f"[TUNNEL] Stopped tunnels for clientId={clientId}")
        else:
            writer.write(b"NO_ACTIVE_TUNNELS\n")
            print(f"[TUNNEL] No active tunnels found for clientId={clientId}")
        
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        return

    else:
        writer.write(b"ERROR Invalid command\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

# =====================================
# 4. Запуск локального прокси-сервера (rp)
# =====================================
async def start_proxy_server(rp):
    """Запускает «тоннельный» прокси, слушающий на rp, для конкретного клиента."""
    try:
        srv = await asyncio.start_server(
            lambda r, w: handle_incoming_connection(r, w, rp),
            host='0.0.0.0',
            port=rp
        )
        print(f"[PROXY] Listening on {rp}")
        asyncio.create_task(srv.serve_forever())
        return True, srv
    except Exception as e:
        print("[start_proxy_server] Error:", e)
        traceback.print_exc()
        return False, None

async def handle_incoming_connection(proxy_reader, proxy_writer, rp):
    addr = proxy_writer.get_extra_info('peername')
    print(f"[PROXY] New external conn from {addr} -> rp={rp}")
    try:
        if rp not in active_tunnels:
            print(f"[handle_incoming_connection] rp={rp} not in active_tunnels!")
            return
        ctrl_reader, ctrl_writer, srv, clientId, subdomain = active_tunnels[rp]

        # Запрашиваем создание data-соединения
        cmd = f"REQUEST_NEW_DATA_CONNECTION {rp} {clientId}\n"
        ctrl_writer.write(cmd.encode())
        await ctrl_writer.drain()

        # Ждём
        data_rw = await asyncio.wait_for(data_connections[rp][clientId].get(), timeout=20)
        data_reader, data_writer = data_rw

        # Прокачиваем в обе стороны
        await asyncio.gather(
            copy_stream(proxy_reader, data_writer),
            copy_stream(data_reader, proxy_writer)
        )

        data_writer.close()
        try:
            await data_writer.wait_closed()
        except:
            pass
    except Exception as e:
        print("[handle_incoming_connection] Error:", e)
        traceback.print_exc()
    finally:
        try:
            proxy_writer.close()
            await proxy_writer.wait_closed()
        except:
            pass
        print(f"[PROXY] External conn {addr} closed")

# =====================================
# 5. ПРОКСИ ДЛЯ ПОДДОМЕНОВ *.yaridiska.ru
#    Парсим Minecraft Handshake
# =====================================
async def start_subdomain_proxy(mc_port=25565):
    try:
        srv = await asyncio.start_server(
            handle_subdomain_connection,
            host='0.0.0.0',
            port=mc_port
        )
        print(f"[SUBDOMAIN_PROXY] Listening on {mc_port} for *.yaridiska.ru")
        await srv.serve_forever()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[SUBDOMAIN_PROXY] Error on port {mc_port}:", e)

async def handle_subdomain_connection(client_reader, client_writer):
    addr = client_writer.get_extra_info('peername')
    try:
        # 1) Парсим handshake
        packet_data = await read_full_minecraft_packet(client_reader)
        if not packet_data:
            print(f"[SUBDOMAIN_PROXY] Empty handshake from {addr}, closing...")
            client_writer.close()
            await client_writer.wait_closed()
            return
        
        domain = parse_minecraft_handshake_get_domain(packet_data)
        if not domain:
            print(f"[SUBDOMAIN_PROXY] Could not parse domain from handshake, closing {addr}")
            await send_minecraft_disconnect(client_writer, reason="Invalid handshake")
            return

        domain_lower = domain.lower()
        
        
        if not domain_lower.endswith(".yaridiska.ru"):
            print(f"[SUBDOMAIN_PROXY] Domain '{domain}' is not *.yaridiska.ru, closing {addr}")
            await send_minecraft_disconnect(client_writer, reason="Wrong domain")
            return

        # Выделяем сам поддомен
        subdom = domain_lower[: -len(".yaridiska.ru")]

        if subdom not in subdomain_to_rp:
            print(f"[SUBDOMAIN_PROXY] No active tunnel for subdomain '{subdom}', closing {addr}")
            await send_minecraft_disconnect(client_writer, reason="Subdomain not active")
            return

        rp = subdomain_to_rp[subdom]
        print(f"[SUBDOMAIN_PROXY] {addr} requested subdomain='{subdom}' => rp={rp}")

        # 2) Подключаемся к локальному rp
        remote_reader, remote_writer = await asyncio.open_connection('127.0.0.1', rp)

        # 3) Нужно передать уже считанный handshake-пакет удалённому серверу
        remote_writer.write(packet_data)
        await remote_writer.drain()

        # 4) Прокачиваем данные
        await asyncio.gather(
            copy_stream(client_reader, remote_writer),
            copy_stream(remote_reader, client_writer)
        )

        remote_writer.close()
        try:
            await remote_writer.wait_closed()
        except:
            pass
        print(f"[SUBDOMAIN_PROXY] Done forwarding for {addr} subdomain='{subdom}'")

    except Exception as e:
        print(f"[SUBDOMAIN_PROXY] Error with {addr}:", e)
        traceback.print_exc()
    finally:
        try:
            client_writer.close()
            await client_writer.wait_closed()
        except:
            pass

# =============== Вспомогательные функции Minecraft handshake ===============

async def read_full_minecraft_packet(reader):

    length_bytes = []
    for _ in range(5):
        byte = await reader.read(1)
        if not byte:
            return None
        length_bytes.append(byte[0])
        if byte[0] & 0x80 == 0:
            break
    if not length_bytes:
        return None
    packet_length = decode_varint(length_bytes)
    if packet_length <= 0:
        return None

    body = await reader.readexactly(packet_length)
    if len(body) != packet_length:
        return None

    return bytes(length_bytes) + body

def parse_minecraft_handshake_get_domain(packet_data):

    offset = 0
    _, varint_len_size = read_varint_from_bytes(packet_data, 0)
    offset += varint_len_size

    packet_id, size_pid = read_varint_from_bytes(packet_data, offset)
    offset += size_pid

    if packet_id != 0x00:
        return None
    
    _, size_proto = read_varint_from_bytes(packet_data, offset)
    offset += size_proto

    str_len, size_str_len = read_varint_from_bytes(packet_data, offset)
    offset += size_str_len

    end_of_str = offset + str_len
    if end_of_str > len(packet_data):
        return None

    server_address_raw = packet_data[offset:end_of_str]
    offset = end_of_str
    try:
        server_address = server_address_raw.decode('utf-8')
    except:
        return None

    if offset + 2 > len(packet_data):
        return None
    offset += 2

    return server_address

def decode_varint(byte_list):
    value = 0
    position = 0
    for b in byte_list:
        value |= (b & 0x7F) << position
        if (b & 0x80) == 0:
            break
        position += 7
    return value

def read_varint_from_bytes(data, offset):
    value = 0
    position = 0
    bytes_read = 0
    while True:
        if offset + bytes_read >= len(data):
            return (None, bytes_read)
        b = data[offset + bytes_read]
        bytes_read += 1
        value |= (b & 0x7F) << position
        if (b & 0x80) == 0:
            break
        position += 7
        if position >= 35:
            return (None, bytes_read)
    return (value, bytes_read)

async def send_minecraft_disconnect(writer, reason="No tunnel"):
    writer.close()
    await writer.wait_closed()

# =====================================
# 6. КОПИРОВАНИЕ ПОТОКОВ
# =====================================
async def copy_stream(r, w):
    try:
        while True:
            buf = await r.read(8192)
            if not buf:
                break
            w.write(buf)
            await w.drain()
    except (BrokenPipeError, ConnectionResetError):
        pass
    except Exception as e:
        print("[copy_stream] Unexpected error:", e)

# =====================================
# 7. ПОИСК СВОБОДНОГО ПОРТА
# =====================================
def find_free_port(rng):
    for p in range(rng[0], rng[1] + 1):
        if p not in active_tunnels and not is_port_in_use(p):
            return p
    return None

def is_port_in_use(port):
    with socket.socket() as s:
        try:
            s.bind(('0.0.0.0', port))
            return False
        except:
            return True

# =====================================
# 8. PING ТУННЕЛЕЙ И ЗАКРЫТИЕ
# =====================================
async def ping_tunnels():
    while True:
        await asyncio.sleep(60)
        if not active_tunnels:
            continue
        print("[PING] Отправка PING всем активным туннелям")
        for rp, (reader, writer, srv, clientId, subdomain) in list(active_tunnels.items()):
            try:
                writer.write(b"PING\n")
                await writer.drain()
            except Exception as e:
                print(f"[PING] Error sending PING to rp={rp}, clientId={clientId}, sub={subdomain}: {e}")
                traceback.print_exc()
                await close_tunnel(rp)

async def close_tunnel(rp):
    """Закрыть туннель на порту rp и удалить из subdomain_to_rp, если нужно."""
    if rp in active_tunnels:
        reader, writer, srv, clientId, subdomain = active_tunnels[rp]
        try:
            writer.write(b"ERROR PING_FAILED\n")
            await writer.drain()
        except:
            pass
        try:
            writer.close()
            await writer.wait_closed()
        except:
            pass

        srv.close()
        await srv.wait_closed()

        del active_tunnels[rp]

        if subdomain in subdomain_to_rp:
            if subdomain_to_rp[subdomain] == rp:
                del subdomain_to_rp[subdomain]

        print(f"[CLOSE] Туннель rp={rp}, subdomain={subdomain}, clientId={clientId} закрыт")

# =====================================
# 9. ЧТЕНИЕ КОНСОЛИ
# =====================================
async def read_console():
    while True:
        try:
            command = await asyncio.to_thread(input)
            if command.strip().lower() == 'exit':
                print("[SERVER] Получена команда выхода. Завершаем работу...")
                save_subdomains()
                for rp in list(active_tunnels.keys()):
                    await close_tunnel(rp)
                if server:
                    server.close()
                    await server.wait_closed()
                os._exit(0)
        except EOFError:
            break
        except Exception as e:
            print(f"[CONSOLE] Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
