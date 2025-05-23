# Pipeline de Dados F1 para Supabase

Este projeto implementa um pipeline ETL (Extract, Transform, Load) que coleta dados em tempo real da Fórmula 1 e os armazena em um banco de dados Supabase. O pipeline é projetado para funcionar de forma contínua, processando os dados à medida que se tornam disponíveis.

## Estrutura do Projeto

- `main_supabase.py`: Aplicação principal que orquestra o pipeline ETL
- `extractor.py`: Extrai dados brutos da F1 usando fastf1_livetiming
- `transformer.py`: Transforma os dados brutos em modelos estruturados
- `supabase_loader.py`: Carrega os dados transformados no Supabase
- `config_supabase.py`: Configurações para conexão com o Supabase
- `models.py`: Classes que representam os diferentes tipos de dados da F1
- `test_supabase.py`: Script para testar a conexão com o Supabase

## Pré-requisitos

- Python 3.7+
- Conta no Supabase com acesso às credenciais
- Biblioteca fastf1_livetiming instalada

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
DB_PASSWORD=sua-senha-db

# Configuração do arquivo de dados F1
F1_DATA_FILE=f1_data.txt

# Intervalo de lote em milissegundos
BATCH_INTERVAL_MS=100
```

## Utilização

### Executar o Pipeline

Para iniciar o pipeline:

```bash
python main_supabase.py
```

O pipeline começará a coletar dados da F1 em tempo real e armazená-los no Supabase.

### Monitoramento

O pipeline gera logs detalhados que são exibidos no console e também salvos no arquivo `f1_pipeline.log`. Esses logs incluem informações sobre a quantidade de dados processados e estatísticas de desempenho.

## Estrutura do Banco de Dados

O pipeline cria as seguintes tabelas no Supabase:

- `sessions`: Informações sobre as sessões de corrida
- `drivers`: Dados dos pilotos
- `lap_data`: Tempos de volta e dados de setor
- `positions`: Posições dos carros na pista
- `telemetry`: Dados telemétricos dos carros
- `race_control`: Mensagens do controle de corrida
- `weather`: Dados meteorológicos

## Encerramento Gracioso

O pipeline foi projetado para encerrar graciosamente quando recebe sinais SIGINT (Ctrl+C) ou SIGTERM. Isso garante que todas as conexões com o banco de dados sejam fechadas corretamente e que não haja perda de dados.

## Resolução de Problemas

Se encontrar problemas ao executar o pipeline, verifique:

1. Se as credenciais do Supabase estão corretas no arquivo `.env`
2. Se a biblioteca fastf1_livetiming está instalada corretamente
3. Se há uma conexão ativa com a internet
4. Os logs em `f1_pipeline.log` para mensagens de erro detalhadas

## Personalização

Você pode personalizar o comportamento do pipeline editando os seguintes parâmetros no arquivo `.env`:

- `BATCH_INTERVAL_MS`: Intervalo entre os lotes de processamento (em milissegundos)
- `F1_TIMEOUT`: Tempo máximo de execução da extração (em segundos)
- `F1_DATA_FILE`: Caminho para o arquivo onde os dados brutos serão armazenados

## Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo LICENSE para mais detalhes.