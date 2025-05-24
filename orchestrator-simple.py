#!/usr/bin/env python3
"""
Orquestrador simples para executar múltiplos monitores
"""

import asyncio
import subprocess
import sys
import signal
import os
from datetime import datetime

# Processos em execução
running_processes = []

def signal_handler(signum, frame):
    """Encerra todos os processos quando recebe sinal de parada"""
    print("\n🛑 Encerrando todos os monitores...")
    for process in running_processes:
        if process.poll() is None:
            process.terminate()
    sys.exit(0)

async def run_monitor(monitor_name: str, session_id: int, input_file: str):
    """Executa um monitor específico"""
    script_map = {
        'weather': 'monitor_weather.py',
        'telemetry': 'monitor_car_telemetry.py',
        'positions': 'monitor_car_positions.py',
        'control': 'monitor_race_control.py'
    }
    
    if monitor_name not in script_map:
        print(f"❌ Monitor '{monitor_name}' não reconhecido")
        return
    
    script = script_map[monitor_name]
    
    if not os.path.exists(script):
        print(f"❌ Script {script} não encontrado")
        return
    
    cmd = [sys.executable, script, '--session-id', str(session_id), '--input-file', input_file]
    
    print(f"🚀 Iniciando {monitor_name} monitor...")
    process = subprocess.Popen(cmd)
    running_processes.append(process)
    
    return process

async def main():
    """Função principal"""
    if len(sys.argv) < 2:
        print("Uso: python orchestrate_monitors.py <session_id> [input_file] [monitors]")
        print("Exemplos:")
        print("  python orchestrate_monitors.py 123")
        print("  python orchestrate_monitors.py 123 f1_data_q1.txt")
        print("  python orchestrate_monitors.py 123 f1_data_q1.txt weather,positions")
        sys.exit(1)
    
    session_id = int(sys.argv[1])
    input_file = sys.argv[2] if len(sys.argv) > 2 else 'f1_data_q1.txt'
    
    # Monitores a executar (padrão: todos)
    if len(sys.argv) > 3:
        monitors = sys.argv[3].split(',')
    else:
        monitors = ['weather', 'telemetry', 'positions', 'control']
    
    # Registra handler para encerramento gracioso
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print(f"📊 Orquestrador de Monitores F1")
    print(f"📁 Session ID: {session_id}")
    print(f"📄 Arquivo de entrada: {input_file}")
    print(f"🔧 Monitores: {', '.join(monitors)}")
    print(f"⏰ Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)
    
    # Verifica se o arquivo existe
    if not os.path.exists(input_file):
        print(f"⚠️  Arquivo {input_file} não encontrado!")
        print("   Os monitores aguardarão a criação do arquivo...")
    
    # Inicia todos os monitores
    tasks = []
    for monitor in monitors:
        process = await run_monitor(monitor, session_id, input_file)
        if process:
            tasks.append(process)
    
    if not tasks:
        print("❌ Nenhum monitor foi iniciado")
        return
    
    print(f"\n✅ {len(tasks)} monitores em execução")
    print("   Pressione Ctrl+C para encerrar todos\n")
    
    # Aguarda todos os processos
    try:
        while True:
            # Verifica se algum processo terminou
            for i, process in enumerate(running_processes):
                if process.poll() is not None:
                    print(f"⚠️  Monitor {i+1} terminou com código {process.returncode}")
            
            await asyncio.sleep(5)
            
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    asyncio.run(main())