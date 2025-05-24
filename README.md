# Pipeline de Dados F1 para Supabase

Este projeto implementa um pipeline ETL (Extract, Transform, Load) que coleta dados em tempo real da Fórmula 1 e os armazena em um banco de dados Supabase. O pipeline foi ajustado para usar **tabelas existentes** ao invés de criar novas tabelas automaticamente.

## Estrutura do Projeto

- `main.py`: Aplicação principal focada na extração e processamento de dados meteorológicos
- `extractor.py`: Extrai dados brutos da F1 usando fastf1_livetiming
- `transformer.py`: Transforma os dados brutos em modelos estruturados
- `supabase_loader.py`: Carrega os dados transformados no Supabase (usando tabelas existentes)
- `config_supabase.py`: Configurações para conexão com o Supabase
- `models.py`: Classes que representam os diferentes tipos de dados da F1
- `monitor_*.py`: Scripts especializados para monitorar tipos específicos de dados

## Tabelas Utilizadas (Existentes)

O pipeline utiliza as seguintes tabelas que **devem existir** no Supabase:

### Tabela `sessions`
```sql
CREATE TABLE public.sessions (
    id serial NOT NULL,
    race_id integer NOT NULL,
    key integer NOT NULL,
    type text NOT NULL,
    name text NOT NULL,
    start_date timestamp with time zone NULL,
    end_date timestamp with time zone NULL,
    gmt_offset text NULL,
    path text NULL,
    created_at timestamp with time zone NULL DEFAULT now(),
    updated_at timestamp with time zone NULL DEFAULT now(),
    CONSTRAINT sessions_pkey PRIMARY KEY (id),
    CONSTRAINT sessions_key_key UNIQUE (key)
);
```

### Tabela `weather_data`
```sql
CREATE TABLE public.weather_data (
    id serial NOT NULL,
    session_id integer NULL,
    timestamp timestamp without time zone NOT NULL,
    air_temp numeric(5, 2) NULL,
    track_temp numeric(5, 2) NULL,
    humidity numeric(5, 2) NULL,
    pressure numeric(6, 2) NULL,
    rainfall numeric(5, 2) NULL,
    wind_direction integer NULL,
    wind_speed numeric(5, 2) NULL,
    created_at timestamp without time zone NULL DEFAULT now(),
    updated_at timestamp without time zone NULL DEFAULT now(),
    CONSTRAINT weather_data_pkey PRIMARY KEY (id),
    CONSTRAINT weather_data_session_id_fkey FOREIGN KEY (session_id) REFERENCES sessions(id)
);
```

### Outras Tabelas Necessárias
- `session_drivers`: Dados dos pilotos por sessão
- `driver_positions`: Posições dos pilotos durante a corrida  
- `car_positions`: Posições 3D dos carros na pista
- `car_telemetry`: Dados telemétricos dos carros (RPM, velocidade, etc.)
- `race_control_messages`: Mensagens do controle de corrida
- `team_radio`: Comunicações de rádio das equipes

## Pré-requisitos

- Python 3.7+
- Conta no Supabase com acesso às credenciais
- Biblioteca fastf1_livetiming instalada
- **Tabelas existentes** no banco de dados Supabase

## Instalação

1. Clone o repositório:
```bash
git clone https://github.com/seu-usuario/f1-supabase-pipeline.git
cd f1-supabase-pipeline
```

2. Instale as dependências:
```bash
pip install -r requirements.txt
```

3. Configure o arquivo `.env` com suas credenciais do Supabase:
```
# Configurações do Supabase
VITE_SUPABASE_URL=https://seu-projeto.supabase.co
VITE_SUPABASE_ANON_KEY=sua-chave-anon

# Configuração para conexão PostgreSQL direta
DB_HOST=db.seu-projeto.supabase.co
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=sua-senha-db

# Configuração do arquivo de dados F1
F1_DATA_FILE=f1_data.txt

# Intervalo de lote em milissegundos
BATCH_INTERVAL_MS=100
```

## Utilização

### Executar o Pipeline Principal (Dados Meteorológicos)

Para iniciar o pipeline com foco em dados meteorológicos:

```bash
python main.py --session-id 123 --verbose
```

Parâmetros:
- `--session-id`: ID da sessão no banco de dados (obrigatório)
- `--output-file`: Arquivo de saída dos dados brutos (opcional)
- `--verbose`: Modo verboso com mais logs (opcional)

### Monitoramento Especializado

Para monitorar tipos específicos de dados:

```bash
# Dados meteorológicos
python monitor_weather.py --session-id 123 --input-file f1_data.txt

# Telemetria dos carros
python monitor_car_telemetry.py --session-id 123 --input-file f1_data.txt

# Posições dos carros
python monitor_car_positions.py --session-id 123 --input-file f1_data.txt

# Mensagens de controle de corrida
python monitor_race_control.py --session-id 123 --input-file f1_data.txt
```

## Mudanças Importantes

### ⚠️ Não Cria Tabelas Automaticamente
O pipeline foi ajustado para **não criar tabelas automaticamente**. As tabelas devem existir previamente no banco de dados Supabase com a estrutura correta.

### ✅ Verificação de Estrutura
O pipeline verifica se as tabelas necessárias existem e se têm a estrutura esperada antes de começar a inserir dados.

### 🔧 Campos Corretos
As funções foram ajustadas para usar os campos corretos das tabelas existentes:
- Tabela `sessions`: usa campos `id`, `key`, `name`, `type`, etc.
- Tabela `weather_data`: usa `session_id`, `timestamp` (without time zone), campos numéricos, etc.

## Monitoramento

O pipeline gera logs detalhados que são exibidos no console e também salvos em arquivos de log:
- `f1_extraction.log`: Log principal da extração
- `f1_weather_extractor.log`: Log específico dos dados meteorológicos
- `f1_telemetry.log`: Log da telemetria dos carros
- `f1_positions.log`: Log das posições dos carros
- `f1_race_control.log`: Log das mensagens de controle

## Encerramento Gracioso

O pipeline foi projetado para encerrar graciosamente quando recebe sinais SIGINT (Ctrl+C) ou SIGTERM. Isso garante que todas as conexões com o banco de dados sejam fechadas corretamente e que não haja perda de dados.

## Análise de Dados

Para analisar o formato dos dados capturados:

```bash
python analyze_f1_data.py f1_data.txt WeatherData 3
```

## Resolução de Problemas

Se encontrar problemas ao executar o pipeline, verifique:

1. **Tabelas existem**: Certifique-se de que todas as tabelas necessárias existem no Supabase
2. **Estrutura correta**: Verifique se a estrutura das tabelas está correta
3. **Credenciais**: Se as credenciais do Supabase estão corretas no arquivo `.env`
4. **Session ID**: Se o `session_id` especificado existe na tabela `sessions`
5. **Conexão**: Se há uma conexão ativa com a internet
6. **Logs**: Consulte os logs para mensagens de erro detalhadas

## Personalização

Você pode personalizar o comportamento do pipeline editando os seguintes parâmetros no arquivo `.env`:

- `BATCH_INTERVAL_MS`: Intervalo entre os lotes de processamento (em milissegundos)
- `F1_TIMEOUT`: Tempo máximo de execução da extração (em segundos)
- `F1_DATA_FILE`: Caminho para o arquivo onde os dados brutos serão armazenados

## Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo LICENSE para mais detalhes.