#!/usr/bin/env python3
"""
Dashboard simples para monitorar o progresso do pipeline F1
"""

import asyncio
import asyncpg
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import sys
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.layout import Layout

# Carrega variáveis de ambiente
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

console = Console()

class F1Dashboard:
    def __init__(self, session_id: int = None):
        self.session_id = session_id
        self.conn = None
        self.conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    async def connect(self):
        """Conecta ao banco de dados"""
        try:
            self.conn = await asyncpg.connect(dsn=self.conn_string, ssl="require")
        except Exception as e:
            console.print(f"[red]Erro ao conectar: {e}[/red]")
            raise
    
    async def get_stats(self):
        """Obtém estatísticas das tabelas"""
        stats = {}
        
        tables = [
            ('weather_data', '🌤️  Meteorologia'),
            ('car_positions', '📍 Posições'),
            ('car_telemetry', '🏎️  Telemetria'),
            ('race_control_messages', '🏁 Controle'),
            ('driver_positions', '👨‍✈️ Pilotos'),
        ]
        
        for table, label in tables:
            try:
                if self.session_id:
                    # Total de registros
                    total = await self.conn.fetchval(
                        f"SELECT COUNT(*) FROM public.{table} WHERE session_id = $1",
                        self.session_id
                    )
                    
                    # Registros nos últimos 5 minutos
                    recent = await self.conn.fetchval(f"""
                        SELECT COUNT(*) FROM public.{table} 
                        WHERE session_id = $1 AND created_at > $2
                    """, self.session_id, datetime.now() - timedelta(minutes=5))
                    
                    # Último registro
                    last_record = await self.conn.fetchval(f"""
                        SELECT MAX(created_at) FROM public.{table}
                        WHERE session_id = $1
                    """, self.session_id)
                else:
                    total = await self.conn.fetchval(f"SELECT COUNT(*) FROM public.{table}")
                    recent = await self.conn.fetchval(f"""
                        SELECT COUNT(*) FROM public.{table} 
                        WHERE created_at > $1
                    """, datetime.now() - timedelta(minutes=5))
                    last_record = await self.conn.fetchval(f"SELECT MAX(created_at) FROM public.{table}")
                
                stats[label] = {
                    'total': total or 0,
                    'recent': recent or 0,
                    'last': last_record.strftime('%H:%M:%S') if last_record else 'N/A'
                }
            except Exception as e:
                stats[label] = {
                    'total': 0,
                    'recent': 0,
                    'last': 'Erro'
                }
        
        return stats
    
    async def get_session_info(self):
        """Obtém informações da sessão"""
        if not self.session_id:
            return None
        
        try:
            session = await self.conn.fetchrow("""
                SELECT s.*, r.name as race_name
                FROM public.sessions s
                LEFT JOIN public.races r ON s.race_id = r.id
                WHERE s.id = $1
            """, self.session_id)
            
            return dict(session) if session else None
        except:
            return None
    
    def create_dashboard(self, stats, session_info):
        """Cria o layout do dashboard"""
        layout = Layout()
        
        # Header
        header = Panel(
            f"[bold cyan]F1 Data Pipeline Dashboard[/bold cyan]\n"
            f"[dim]{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/dim]",
            style="bright_blue"
        )
        
        # Session info
        if session_info:
            session_panel = Panel(
                f"[bold]Session:[/bold] {session_info.get('name', 'N/A')}\n"
                f"[bold]Type:[/bold] {session_info.get('type', 'N/A')}\n"
                f"[bold]Race:[/bold] {session_info.get('race_name', 'N/A')}",
                title="📋 Session Info",
                style="green"
            )
        else:
            session_panel = Panel(
                "[yellow]Monitorando todas as sessões[/yellow]",
                title="📋 Session Info"
            )
        
        # Stats table
        table = Table(title="📊 Estatísticas em Tempo Real")
        table.add_column("Monitor", style="cyan", width=20)
        table.add_column("Total", justify="right", style="green")
        table.add_column("Últimos 5min", justify="right", style="yellow")
        table.add_column("Última Atualização", style="magenta")
        
        for label, data in stats.items():
            table.add_row(
                label,
                str(data['total']),
                str(data['recent']),
                data['last']
            )
        
        # Layout
        layout.split_column(
            Layout(header, size=5),
            Layout(session_panel, size=6),
            Layout(table)
        )
        
        return layout
    
    async def run(self):
        """Executa o dashboard"""
        await self.connect()
        
        with Live(console=console, refresh_per_second=1) as live:
            while True:
                try:
                    stats = await self.get_stats()
                    session_info = await self.get_session_info()
                    
                    dashboard = self.create_dashboard(stats, session_info)
                    live.update(dashboard)
                    
                    await asyncio.sleep(5)  # Atualiza a cada 5 segundos
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    console.print(f"[red]Erro: {e}[/red]")
                    await asyncio.sleep(5)
        
        if self.conn:
            await self.conn.close()

async def main():
    """Função principal"""
    session_id = int(sys.argv[1]) if len(sys.argv) > 1 else None
    
    console.print("[bold green]Iniciando F1 Dashboard...[/bold green]")
    
    if session_id:
        console.print(f"[cyan]Monitorando sessão {session_id}[/cyan]")
    else:
        console.print("[yellow]Monitorando todas as sessões[/yellow]")
    
    dashboard = F1Dashboard(session_id)
    
    try:
        await dashboard.run()
    except KeyboardInterrupt:
        console.print("\n[yellow]Dashboard encerrado[/yellow]")
    except Exception as e:
        console.print(f"[red]Erro fatal: {e}[/red]")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass