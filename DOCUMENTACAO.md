# Documentação do Sistema de Análise de Importações

## 1. Visão Geral do Sistema

O sistema é uma aplicação web desenvolvida em Streamlit que permite analisar dados de importações brasileiras, identificar possíveis importadores e realizar análises estatísticas. O sistema é composto por três componentes principais:

1. **Interface Web (Streamlit)**
   - Busca de empresas por CNPJ
   - Visualização de dados de importação
   - Análises estatísticas
   - Interface para consultas SQL personalizadas
   - Exportação de dados

2. **Banco de Dados (SQLite)**
   - Armazenamento de dados de importações
   - Tabelas de empresas e importações
   - Índices otimizados para consultas

3. **Sistema de Machine Learning**
   - Identificação de possíveis importadores
   - Análise de padrões de importação
   - Recomendação de empresas

## 2. Fluxo de Dados

### 2.1 Processamento de Dados
1. **Coleta de Dados**
   - Dados são coletados de fontes oficiais (SISCOMEX)
   - Processados e normalizados
   - Armazenados no banco SQLite

2. **Estrutura do Banco**
   - Tabela `empresas`: Informações cadastrais
   - Tabela `importacoes`: Dados de importações
   - Relacionamentos e índices otimizados

### 2.2 Fluxo de Machine Learning

1. **Preparação dos Dados**
   ```python
   # Exemplo de preparação
   dados_ml = {
       'volume_importacoes': volume_total,
       'frequencia_importacoes': frequencia,
       'diversidade_produtos': len(produtos_unicos),
       'valor_medio': valor_medio,
       'tamanho_empresa': tamanho_empresa
   }
   ```

2. **Processo de Decisão**
   - Análise de múltiplos fatores:
     - Volume de importações
     - Frequência de importações
     - Diversidade de produtos
     - Valor médio das importações
     - Tamanho da empresa

3. **Cálculo de Probabilidade**
   - Cada fator recebe um peso específico
   - Fórmula de cálculo:
     ```
     Probabilidade = (w1 * volume + w2 * frequencia + w3 * diversidade + w4 * valor + w5 * tamanho) / soma_pesos
     ```
   - Onde:
     - w1, w2, w3, w4, w5 são os pesos de cada fator
     - Os valores são normalizados para escala 0-1

4. **Classificação**
   - Empresas são classificadas em categorias:
     - Alta probabilidade (score > 0.7)
     - Média probabilidade (0.4 < score < 0.7)
     - Baixa probabilidade (score < 0.4)

## 3. Interface do Usuário

### 3.1 Funcionalidades Principais

1. **Busca por CNPJ**
   - Busca rápida de empresas
   - Visualização de dados cadastrais
   - Histórico de importações

2. **Análise de Importações**
   - Gráficos de volume
   - Análise temporal
   - Estatísticas por produto

3. **Consultas Personalizadas**
   - Interface SQL
   - Exportação de resultados
   - Filtros personalizados

### 3.2 Visualizações

1. **Gráficos**
   - Volume de importações
   - Distribuição de produtos
   - Análise temporal
   - Comparativos

2. **Tabelas**
   - Dados detalhados
   - Estatísticas
   - Resultados de ML

## 4. Sistema de Machine Learning

### 4.1 Fatores de Análise

1. **Volume de Importações**
   - Total de importações
   - Valor total
   - Quantidade de produtos

2. **Frequência**
   - Regularidade das importações
   - Intervalos entre importações
   - Consistência temporal

3. **Diversidade**
   - Número de produtos únicos
   - Variedade de origens
   - Complexidade das operações

4. **Valor**
   - Valor médio por importação
   - Valor total
   - Distribuição de valores

5. **Tamanho da Empresa**
   - Faturamento
   - Número de funcionários
   - Capacidade operacional

### 4.2 Processo de Decisão

1. **Pré-processamento de Dados**
   ```python
   # Exemplo de pré-processamento
   dados_empresa = {
       'importacoes': [...],
       'produtos': [...],
       'valores': [...],
       'datas': [...]
   }
   ```

2. **Clustering e Redução de Dimensionalidade**
   - Uso de K-means para agrupar empresas similares
   - 9 clusters para classificação
   - Normalização de features numéricas
   - Distribuição de classes após clustering:
     - Classe 0: 6566 empresas (95.6%)
     - Classe 6: 218 empresas (3.2%)
     - Outras classes: <1% cada

3. **Processamento de Features**
   ```python
   # Exemplo de processamento
   features = {
       'volume': calcular_volume(dados_empresa),
       'frequencia': calcular_frequencia(dados_empresa),
       'diversidade': calcular_diversidade(dados_empresa),
       'valor': calcular_valor_medio(dados_empresa),
       'tamanho': calcular_tamanho(dados_empresa)
   }
   ```

4. **Vetorização de Texto**
   - Uso de TF-IDF para descrição de produtos
   - Máximo de 100 features
   - N-grams de 1 a 2 palavras

5. **Balanceamento de Classes**
   - Cálculo de pesos para classes desbalanceadas
   - Uso de `compute_class_weight` do sklearn
   - Ajuste automático de pesos no treinamento

6. **Modelo XGBoost**
   ```python
   # Configuração do modelo
   clf = XGBClassifier(
       n_estimators=200,
       max_depth=6,
       learning_rate=0.1,
       subsample=0.8,
       colsample_bytree=0.8,
       random_state=42,
       tree_method='hist'
   )
   ```

7. **Métricas de Avaliação**
   - Acurácia de Treino: 99.91%
   - Acurácia de Teste: 98.84%
   - Cross-validation: 99.26% (±0.56%)
   - Relatório de classificação por classe:
     - Precision, Recall e F1-score
     - Suporte para cada classe
     - Médias macro e weighted

8. **Classificação Final**
   ```python
   # Exemplo de classificação
   if score > 0.7:
       categoria = "Alta Probabilidade"
   elif score > 0.4:
       categoria = "Média Probabilidade"
   else:
       categoria = "Baixa Probabilidade"
   ```

### 4.3 Manutenção do Modelo

1. **Atualização Regular**
   - Retreinamento com novos dados
   - Recalibração de clusters
   - Ajuste de hiperparâmetros

2. **Monitoramento**
   - Acompanhamento de métricas
   - Detecção de drift
   - Validação de resultados

3. **Otimização**
   - Ajuste de pesos das classes
   - Refinamento de features
   - Melhoria de performance

## 5. Manutenção e Atualização

### 5.1 Banco de Dados
- Backup regular
- Otimização de índices
- Limpeza de dados

### 5.2 Sistema ML
- Atualização de pesos
- Recalibração de modelos
- Validação de resultados

### 5.3 Interface
- Atualizações de UI/UX
- Novas funcionalidades
- Correções de bugs

## 6. Requisitos Técnicos

### 6.1 Dependências
```
streamlit
pandas
numpy
plotly
sqlite3
```

### 6.2 Configuração
- Python 3.8+
- Banco SQLite

## 7. Considerações de Segurança

1. **Dados**
   - Proteção de informações sensíveis
   - Backup regular
   - Controle de acesso

2. **Aplicação**
   - Validação de inputs
   - Sanitização de queries
   - Logs de acesso

3. **ML**
   - Validação de resultados
   - Proteção contra vieses
   - Monitoramento de performance 