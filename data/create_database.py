"""
Script para criar banco de dados SQLite com dados de municípios e ponderadores do FUNDEB
"""
import sqlite3
import pandas as pd

# Conectar ao banco
conn = sqlite3.connect('fundeb_data.db')
cursor = conn.cursor()

# Criar tabela de municípios
cursor.execute('''
CREATE TABLE IF NOT EXISTS municipios (
    id INTEGER PRIMARY KEY,
    codigo_ibge TEXT UNIQUE,
    nome TEXT,
    uf TEXT,
    populacao INTEGER,
    nse REAL,  -- Nível Socioeconômico (0-100)
    drec REAL  -- Disponibilidade de Recursos (índice)
)
''')

# Criar tabela de matrículas
cursor.execute('''
CREATE TABLE IF NOT EXISTS matriculas (
    id INTEGER PRIMARY KEY,
    codigo_ibge TEXT,
    etapa TEXT,
    modalidade TEXT,
    quantidade INTEGER,
    FOREIGN KEY (codigo_ibge) REFERENCES municipios(codigo_ibge)
)
''')

# Criar tabela de ponderadores VAAT/VAAF (Portaria MEC 2025)
cursor.execute('''
CREATE TABLE IF NOT EXISTS ponderadores (
    id INTEGER PRIMARY KEY,
    etapa TEXT,
    modalidade TEXT,
    ponderador_vaat REAL,
    ponderador_vaaf REAL,
    multiplicador_indigena_quilombola REAL DEFAULT 1.40,
    multiplicador_rural REAL DEFAULT 1.15,
    adicional_especial REAL DEFAULT 1.40
)
''')

# Dados de 20 municípios representativos (diversos perfis)
municipios_data = [
    # Pequeno porte, baixo NSE
    ('1500107', 'Abaetetuba', 'PA', 157698, 42.5, 0.87),
    ('1200013', 'Acrelândia', 'AC', 15256, 38.2, 0.82),
    ('2700102', 'Água Branca', 'AL', 21364, 35.8, 0.79),

    # Médio porte, NSE médio-baixo
    ('4100202', 'Almirante Tamandaré', 'PR', 117168, 48.9, 0.91),
    ('4300604', 'Alvorada', 'RS', 208177, 45.3, 0.88),
    ('4102307', 'Apucarana', 'PR', 136234, 50.2, 0.93),

    # Médio porte, NSE médio
    ('3503307', 'Araçatuba', 'SP', 198129, 58.4, 1.02),
    ('2901502', 'Barreiras', 'BA', 160569, 55.7, 0.98),
    ('5002704', 'Campo Grande', 'MS', 916001, 62.1, 1.08),

    # Grande porte, NSE médio-alto
    ('2304400', 'Fortaleza', 'CE', 2703391, 59.8, 1.05),
    ('3106200', 'Belo Horizonte', 'MG', 2530701, 67.3, 1.15),
    ('3550308', 'São Paulo', 'SP', 12396372, 72.1, 1.22),

    # Interior, perfis diversos
    ('2927408', 'Salvador', 'BA', 2900319, 61.2, 1.06),
    ('4314902', 'Porto Alegre', 'RS', 1492530, 68.7, 1.18),
    ('5300108', 'Brasília', 'DF', 3094325, 70.5, 1.20),

    # Pequeno porte, NSE variado
    ('1721000', 'Palmas', 'TO', 313349, 56.2, 0.99),
    ('2111300', 'São Luís', 'MA', 1115932, 53.4, 0.96),
    ('1302603', 'Manaus', 'AM', 2255903, 54.8, 0.97),
    ('5103403', 'Cuiabá', 'MT', 623614, 57.9, 1.01),
    ('2800308', 'Aracaju', 'SE', 672614, 60.3, 1.04),
]

cursor.executemany('''
    INSERT OR REPLACE INTO municipios (codigo_ibge, nome, uf, populacao, nse, drec)
    VALUES (?, ?, ?, ?, ?, ?)
''', municipios_data)

# Ponderadores oficiais 2025 (baseados na legislação real)
ponderadores_data = [
    # Creche
    ('Creche', 'Integral', 1.90, 1.55, 1.40, 1.15, 1.40),
    ('Creche', 'Parcial', 1.50, 1.30, 1.40, 1.15, 1.40),

    # Pré-escola
    ('Pré-escola', 'Integral', 1.88, 1.50, 1.40, 1.15, 1.40),
    ('Pré-escola', 'Parcial', 1.40, 1.25, 1.40, 1.15, 1.40),

    # Ensino Fundamental - Anos Iniciais
    ('EF Anos Iniciais', 'Integral', 1.45, 1.30, 1.40, 1.15, 1.40),
    ('EF Anos Iniciais', 'Parcial Urbano', 1.00, 1.00, 1.40, 1.15, 1.40),
    ('EF Anos Iniciais', 'Parcial Rural', 1.20, 1.15, 1.40, 1.15, 1.40),

    # Ensino Fundamental - Anos Finais
    ('EF Anos Finais', 'Integral', 1.40, 1.25, 1.40, 1.15, 1.40),
    ('EF Anos Finais', 'Parcial Urbano', 1.15, 1.10, 1.40, 1.15, 1.40),
    ('EF Anos Finais', 'Parcial Rural', 1.30, 1.20, 1.40, 1.15, 1.40),

    # Ensino Médio
    ('Ensino Médio', 'Integral', 1.55, 1.40, 1.40, 1.15, 1.40),
    ('Ensino Médio', 'Parcial', 1.30, 1.25, 1.40, 1.15, 1.40),

    # EJA
    ('EJA', 'Fundamental', 1.05, 0.95, 1.40, 1.15, 1.40),
    ('EJA', 'Médio', 1.20, 1.10, 1.40, 1.15, 1.40),
]

cursor.executemany('''
    INSERT OR REPLACE INTO ponderadores (etapa, modalidade, ponderador_vaat, ponderador_vaaf,
                                        multiplicador_indigena_quilombola, multiplicador_rural, adicional_especial)
    VALUES (?, ?, ?, ?, ?, ?, ?)
''', ponderadores_data)

# Gerar matrículas realistas para cada município
import random
random.seed(42)

for municipio in municipios_data:
    codigo_ibge = municipio[0]
    populacao = municipio[3]

    # Estimar número de alunos (aprox 15-20% da população)
    total_alunos = int(populacao * random.uniform(0.15, 0.20))

    # Distribuição realista por etapa/modalidade
    matriculas = [
        # Creche (8% do total)
        (codigo_ibge, 'Creche', 'Integral', int(total_alunos * 0.04)),
        (codigo_ibge, 'Creche', 'Parcial', int(total_alunos * 0.04)),

        # Pré-escola (10% do total)
        (codigo_ibge, 'Pré-escola', 'Integral', int(total_alunos * 0.05)),
        (codigo_ibge, 'Pré-escola', 'Parcial', int(total_alunos * 0.05)),

        # EF Anos Iniciais (35% do total)
        (codigo_ibge, 'EF Anos Iniciais', 'Integral', int(total_alunos * 0.10)),
        (codigo_ibge, 'EF Anos Iniciais', 'Parcial Urbano', int(total_alunos * 0.20)),
        (codigo_ibge, 'EF Anos Iniciais', 'Parcial Rural', int(total_alunos * 0.05)),

        # EF Anos Finais (30% do total)
        (codigo_ibge, 'EF Anos Finais', 'Integral', int(total_alunos * 0.08)),
        (codigo_ibge, 'EF Anos Finais', 'Parcial Urbano', int(total_alunos * 0.18)),
        (codigo_ibge, 'EF Anos Finais', 'Parcial Rural', int(total_alunos * 0.04)),

        # Ensino Médio (12% do total)
        (codigo_ibge, 'Ensino Médio', 'Integral', int(total_alunos * 0.04)),
        (codigo_ibge, 'Ensino Médio', 'Parcial', int(total_alunos * 0.08)),

        # EJA (5% do total)
        (codigo_ibge, 'EJA', 'Fundamental', int(total_alunos * 0.03)),
        (codigo_ibge, 'EJA', 'Médio', int(total_alunos * 0.02)),
    ]

    cursor.executemany('''
        INSERT INTO matriculas (codigo_ibge, etapa, modalidade, quantidade)
        VALUES (?, ?, ?, ?)
    ''', matriculas)

conn.commit()

# Verificar dados
print("✅ Banco de dados criado com sucesso!")
print(f"\n📊 Estatísticas:")
print(f"- Municípios: {cursor.execute('SELECT COUNT(*) FROM municipios').fetchone()[0]}")
print(f"- Matrículas: {cursor.execute('SELECT COUNT(*) FROM matriculas').fetchone()[0]}")
print(f"- Ponderadores: {cursor.execute('SELECT COUNT(*) FROM ponderadores').fetchone()[0]}")

# Mostrar exemplo de município
print(f"\n📍 Exemplo - Município de Apucarana:")
cursor.execute('''
    SELECT m.nome, m.uf, m.populacao, m.nse, m.drec,
           SUM(mat.quantidade) as total_alunos
    FROM municipios m
    LEFT JOIN matriculas mat ON m.codigo_ibge = mat.codigo_ibge
    WHERE m.nome = "Apucarana"
    GROUP BY m.codigo_ibge
''')
result = cursor.fetchone()
print(f"  - População: {result[2]:,}")
print(f"  - NSE: {result[3]}")
print(f"  - DRec: {result[4]}")
print(f"  - Total de alunos: {result[5]:,}")

conn.close()
print("\n🎯 Banco pronto para uso!")
