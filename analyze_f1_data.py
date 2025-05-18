import ast
import base64
import sys
import zlib
import binascii
from datetime import datetime

def analyze_data_format(input_file, topic_filter=None, num_samples=5):
    """Analisa o formato dos dados para entender como decodificá-los"""
    print(f"Analisando arquivo: {input_file}")
    
    samples = {}
    with open(input_file, 'r') as f:
        for i, line in enumerate(f):
            try:
                # Parse da linha
                parsed = ast.literal_eval(line)
                
                # Verifica formato esperado
                if not isinstance(parsed, list) or len(parsed) < 3:
                    continue
                
                topic, data, timestamp = parsed[0], parsed[1], parsed[2]
                
                # Filtra pelo tópico se especificado
                if topic_filter and topic != topic_filter:
                    continue
                
                # Armazena amostras
                if topic not in samples:
                    samples[topic] = []
                
                if len(samples[topic]) < num_samples:
                    samples[topic].append((i+1, data, timestamp))
            except Exception as e:
                pass
    
    # Imprime análise
    for topic, topic_samples in samples.items():
        print(f"\n=== TÓPICO: {topic} ===")
        print(f"Amostras encontradas: {len(topic_samples)}")
        
        for i, (line_num, data, timestamp) in enumerate(topic_samples):
            print(f"\nAmostra {i+1} (linha {line_num}):")
            print(f"Timestamp: {timestamp}")
            
            # Dados em formato String
            if isinstance(data, str):
                print(f"Tipo de dado: String (len={len(data)})")
                print(f"Primeiros 50 chars: {data[:50]}...")
                
                # Tenta decodificar como base64
                try:
                    base64_decoded = base64.b64decode(data)
                    print(f"Decodificação base64: Sucesso ({len(base64_decoded)} bytes)")
                    
                    # Tenta descomprimir como zlib
                    try:
                        zlib_decompressed = zlib.decompress(base64_decoded)
                        print(f"Descompressão zlib: Sucesso ({len(zlib_decompressed)} bytes)")
                        print(f"Primeiros 100 bytes decodificados: {zlib_decompressed[:100]}")
                    except Exception as e:
                        print(f"Descompressão zlib: Falha ({e})")
                    
                    # Analisa o início dos dados para identificar o formato
                    print(f"Hexdump dos primeiros 20 bytes: {binascii.hexlify(base64_decoded[:20])}")
                except Exception as e:
                    print(f"Decodificação base64: Falha ({e})")
            
            # Dados em formato Dict
            elif isinstance(data, dict):
                print(f"Tipo de dado: Dict (keys={list(data.keys())[:5]})")
                # Mostra uma amostra do conteúdo
                for k, v in list(data.items())[:3]:
                    print(f"  {k}: {v}")
            
            # Outros formatos
            else:
                print(f"Tipo de dado: {type(data)}")
                print(f"Representação: {str(data)[:100]}...")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python analyze_f1_data.py arquivo.txt [tópico] [num_amostras]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    topic_filter = sys.argv[2] if len(sys.argv) > 2 else None
    num_samples = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    
    analyze_data_format(input_file, topic_filter, num_samples)