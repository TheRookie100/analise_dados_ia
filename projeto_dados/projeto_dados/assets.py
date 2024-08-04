"""import os
import json
import pandas as pd
from datetime import datetime
from dagster import get_dagster_logger, asset
from crawler_noticia.governo.governo.spiders.noticia import G1Spider
from crawler_noticia.economia.economia.spiders.noticia import NoticiasSpider
from scrapy.crawler import CrawlerProcess
from db_mongo.conexao_mongo import salvar_no_mongo, conectar_mongo
import matplotlib.pyplot as plt
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Função para rodar o spider e salvar os dados no MongoDB
def run_spider(spider, collection_name):
    logger = get_dagster_logger()

    # Adiciona um timestamp ao nome do arquivo de saída
    timestamp = datetime.now().strftime("%d%m%Y%H%M%S")
    output_file = f"data/{collection_name}_{timestamp}.json"

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    process = CrawlerProcess(settings={
        "FEED_FORMAT": "json",
        "FEED_URI": output_file
    })

    process.crawl(spider)
    process.start()

    if os.path.exists(output_file):
        with open(output_file, "r", encoding='utf-8') as f:
            data = json.load(f)

            # Adiciona um timestamp a cada documento
            document_timestamp = datetime.now().isoformat()
            for entry in data:
                entry['timestamp'] = document_timestamp

            salvar_no_mongo(data, collection_name)  # Adiciona dados ao MongoDB
            logger.info(f"Novos dados adicionados à coleção {collection_name} no MongoDB")

# Função para salvar dados tratados no MongoDB
def salvar_dados_tratados(dataframe, collection_name):
    db = conectar_mongo()
    colecao_tratada = db[f"{collection_name}_tratados"]  # Nome da coleção para dados tratados
    # Adiciona um timestamp a cada documento
    timestamp = datetime.now().isoformat()
    dataframe['timestamp'] = timestamp
    # Converte o DataFrame para uma lista de dicionários e insere no MongoDB
    colecao_tratada.insert_many(dataframe.to_dict('records'))

# Assets para os crawlers
@asset(description="Coleta dados de notícias de economia.")
def crawler_economia():
    run_spider(NoticiasSpider, "economia")

@asset(description="Coleta dados de notícias de governo.")
def crawler_governo():
    run_spider(G1Spider, "governo")

# Função para tratar os dados de economia (sem análise de sentimento)
def tratar_dados_economia_func(colecao_nome: str) -> pd.DataFrame:
    logger = get_dagster_logger()
    logger.info(f"Iniciando tratamento de dados para a coleção: {colecao_nome}")

    db = conectar_mongo()
    colecao = db[colecao_nome]
    data = pd.DataFrame(list(colecao.find()))
    logger.info(f"Dados coletados da coleção {colecao_nome}: {data.head()}")

    if data.empty:
        raise ValueError(f"A coleção {colecao_nome} está vazia.")

    data = data.drop(columns=['_id'], errors='ignore')
    logger.info(f"Dados após remoção da coluna '_id': {data.head()}")

    # Processa o conteúdo do campo 'body' se ele for um dicionário
    if 'body' in data.columns:
        data['body'] = data['body'].apply(lambda x: x['text'] if isinstance(x, dict) else x)
        logger.info(f"Dados após processamento do campo 'body': {data.head()}")

    # Adiciona a coluna 'target' se não existir
    if 'target' not in data.columns:
        data['target'] = 0  # ou qualquer lógica que você tenha para definir 'target'
    logger.info(f"Dados após adicionar a coluna 'target': {data.head()}")

    # Salva os dados tratados
    salvar_dados_tratados(data, colecao_nome)
    logger.info(f"Dados tratados para {colecao_nome} salvos no MongoDB")

    return data

# Função para tratar os dados de governo (com análise de sentimento)
def tratar_dados_governo_func(colecao_nome: str) -> pd.DataFrame:
    logger = get_dagster_logger()
    logger.info(f"Iniciando tratamento de dados para a coleção: {colecao_nome}")

    db = conectar_mongo()
    colecao = db[colecao_nome]
    data = pd.DataFrame(list(colecao.find()))
    logger.info(f"Dados coletados da coleção {colecao_nome}: {data.head()}")

    if data.empty:
        raise ValueError(f"A coleção {colecao_nome} está vazia.")

    data = data.drop(columns=['_id'], errors='ignore')
    logger.info(f"Dados após remoção da coluna '_id': {data.head()}")

    # Processa o conteúdo do campo 'body' se ele for um dicionário
    if 'body' in data.columns:
        data['body'] = data['body'].apply(lambda x: x['text'] if isinstance(x, dict) else x)
        logger.info(f"Dados após processamento do campo 'body': {data.head()}")

        # Adiciona a análise de sentimentos
        analyzer = SentimentIntensityAnalyzer()
        data['sentimento'] = data['body'].apply(lambda x: analyzer.polarity_scores(x)['compound'] if isinstance(x, str) else None)
        data['sentimento_classificacao'] = data['sentimento'].apply(lambda x: 'positivo' if x > 0 else ('negativo' if x < 0 else 'neutro'))
        logger.info(f"Dados após adicionar a coluna 'sentimento': {data.head()}")

    # Verificação da coluna 'sentimento_classificacao'
    if 'sentimento_classificacao' not in data.columns:
        data['sentimento_classificacao'] = None
        logger.warning(f"A coluna 'sentimento_classificacao' não foi criada para a coleção {colecao_nome}.")

    # Adiciona a coluna 'target' se não existir
    if 'target' not in data.columns:
        data['target'] = 0  # ou qualquer lógica que você tenha para definir 'target'
    logger.info(f"Dados após adicionar a coluna 'target': {data.head()}")

    # Salva os dados tratados
    salvar_dados_tratados(data, colecao_nome)
    logger.info(f"Dados tratados para {colecao_nome} salvos no MongoDB")

    return data

# Assets para tratamento dos dados
@asset 
def tratar_dados_economia() -> pd.DataFrame:
    return tratar_dados_economia_func('economia')

@asset 
def tratar_dados_governo() -> pd.DataFrame:
    return tratar_dados_governo_func('governo')

# Função para gerar gráficos de acurácia
def gerar_grafico_acuracia(nome_modelo, accuracy):
    os.makedirs('resultados', exist_ok=True)
    plt.figure()
    plt.bar([nome_modelo], [accuracy])
    plt.ylabel('Acurácia')
    plt.title(f'Acurácia do modelo {nome_modelo}')
    plt.savefig(f'resultados/{nome_modelo}_acuracia.png')

# Função para gerar gráficos de sentimentos
def gerar_grafico_sentimentos(df, nome_modelo):
    if 'sentimento_classificacao' not in df.columns:
        raise ValueError("A coluna 'sentimento_classificacao' não está presente no DataFrame.")
    sentimentos = df['sentimento_classificacao'].value_counts()
    os.makedirs('resultados', exist_ok=True)
    plt.figure()
    sentimentos.plot(kind='bar')
    plt.ylabel('Quantidade')
    plt.title(f'Sentimentos das notícias - {nome_modelo}')
    plt.savefig(f'resultados/{nome_modelo}_sentimentos.png')

# Assets para treinamento e teste da IA
@asset 
def treinar_ia_economia(tratar_dados_economia: pd.DataFrame) -> None:
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import accuracy_score

    logger = get_dagster_logger()

    # Verifica se há dados suficientes
    if len(tratar_dados_economia) < 2:
        raise ValueError("Dados insuficientes para treinamento e teste")

    # Prepare os dados de treino e teste
    X = tratar_dados_economia.select_dtypes(include=[float, int])
    y = tratar_dados_economia['target']
    if X.empty or y.empty:
        raise ValueError("Dados de entrada estão vazios ou contém colunas não numéricas")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    logger.info(f"Dados de treino e teste preparados")

    # Treine o modelo
    modelo = RandomForestClassifier()
    modelo.fit(X_train, y_train)
    logger.info(f"Modelo treinado com dados de Economia")

    # Teste o modelo
    y_pred = modelo.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    logger.info(f"Modelo de IA treinado com acurácia: {accuracy}")

    # Gerar gráfico de acurácia
    gerar_grafico_acuracia("Economia", accuracy)

@asset 
def treinar_ia_governo(tratar_dados_governo: pd.DataFrame) -> None:
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import accuracy_score

    logger = get_dagster_logger()

    # Verifica se há dados suficientes
    if len(tratar_dados_governo) < 2:
        raise ValueError("Dados insuficientes para treinamento e teste")

    # Prepare os dados de treino e teste
    X = tratar_dados_governo.select_dtypes(include=[float, int])
    y = tratar_dados_governo['target']
    if X.empty or y.empty:
        raise ValueError("Dados de entrada estão vazios ou contém colunas não numéricas")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    logger.info(f"Dados de treino e teste preparados")

    # Treine o modelo
    modelo = RandomForestClassifier()
    modelo.fit(X_train, y_train)
    logger.info(f"Modelo treinado com dados de Governo")

    # Teste o modelo
    y_pred = modelo.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    logger.info(f"Modelo de IA treinado com acurácia: {accuracy}")

    # Gerar gráfico de acurácia
    gerar_grafico_acuracia("Governo", accuracy)

    # Gerar gráfico de sentimentos
    gerar_grafico_sentimentos(tratar_dados_governo, "Governo")"""
  
  
  
import os
import json
import pandas as pd
from datetime import datetime
from dagster import get_dagster_logger, asset
from crawler_noticia.governo.governo.spiders.noticia import G1Spider
from crawler_noticia.economia.economia.spiders.noticia import NoticiasSpider
from scrapy.crawler import CrawlerProcess
from db_mongo.conexao_mongo import salvar_no_mongo, conectar_mongo
import matplotlib.pyplot as plt
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Utilidade para gerar gráficos de acurácia
def gerar_grafico_acuracia(nome_modelo, accuracy):
    os.makedirs('resultados', exist_ok=True)
    plt.figure()
    plt.bar([nome_modelo], [accuracy])
    plt.ylabel('Acurácia')
    plt.title(f'Acurácia do modelo {nome_modelo}')
    plt.savefig(f'resultados/{nome_modelo}_acuracia.png')
    plt.close()

# Utilidade para gerar gráficos de sentimentos
def gerar_grafico_sentimentos(df, nome_modelo):
    if 'sentimento_classificacao' not in df.columns:
        raise ValueError("A coluna 'sentimento_classificacao' não está presente no DataFrame.")
    
    sentimentos = df['sentimento_classificacao'].value_counts()
    os.makedirs('resultados', exist_ok=True)
    plt.figure()
    sentimentos.plot(kind='bar')
    plt.ylabel('Quantidade')
    plt.title(f'Sentimentos das notícias - {nome_modelo}')
    plt.savefig(f'resultados/{nome_modelo}_sentimentos.png')
    plt.close()

# Função para rodar o spider e salvar os dados no MongoDB
def run_spider(spider, collection_name):
    logger = get_dagster_logger()
    timestamp = datetime.now().strftime("%d%m%Y%H%M%S")
    output_file = f"data/{collection_name}_{timestamp}.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    process = CrawlerProcess(settings={"FEED_FORMAT": "json", "FEED_URI": output_file})
    process.crawl(spider)
    process.start()

    if os.path.exists(output_file):
        with open(output_file, "r", encoding='utf-8') as f:
            data = json.load(f)
            document_timestamp = datetime.now().isoformat()
            for entry in data:
                entry['timestamp'] = document_timestamp
            salvar_no_mongo(data, collection_name)
            logger.info(f"Novos dados adicionados à coleção {collection_name} no MongoDB")

# Função para salvar dados tratados no MongoDB
def salvar_dados_tratados(dataframe, collection_name):
    db = conectar_mongo()
    colecao_tratada = db[f"{collection_name}_tratados"]
    timestamp = datetime.now().isoformat()
    dataframe['timestamp'] = timestamp
    colecao_tratada.insert_many(dataframe.to_dict('records'))

# Asset para coletar dados de notícias de economia
@asset(description="Coleta dados de notícias de economia.")
def crawler_economia():
    run_spider(NoticiasSpider, "economia")

# Asset para coletar dados de notícias de governo
@asset(description="Coleta dados de notícias de governo.")
def crawler_governo():
    run_spider(G1Spider, "governo")

# Função genérica para tratamento de dados
def tratar_dados_func(colecao_nome: str, sentiment_analysis: bool = False) -> pd.DataFrame:
    logger = get_dagster_logger()
    db = conectar_mongo()
    colecao = db[colecao_nome]
    data = pd.DataFrame(list(colecao.find()))
    if data.empty:
        raise ValueError(f"A coleção {colecao_nome} está vazia.")
    data = data.drop(columns=['_id'], errors='ignore')
    if 'body' in data.columns:
        data['body'] = data['body'].apply(lambda x: x['text'] if isinstance(x, dict) else x)
    if sentiment_analysis:
        analyzer = SentimentIntensityAnalyzer()
        data['sentimento'] = data['body'].apply(lambda x: analyzer.polarity_scores(x)['compound'] if isinstance(x, str) else None)
        data['sentimento_classificacao'] = data['sentimento'].apply(lambda x: 'positivo' if x > 0 else ('negativo' if x < 0 else 'neutro'))
    data['target'] = 0  # ou qualquer lógica que você tenha para definir 'target'
    salvar_dados_tratados(data, colecao_nome)
    return data

# Asset para processar dados de economia
@asset(description="Processa dados de economia.")
def tratar_dados_economia():
    return tratar_dados_func('economia')

# Asset para processar dados de governo com análise de sentimentos
@asset(description="Processa dados de governo com análise de sentimento.")
def tratar_dados_governo():
    return tratar_dados_func('governo', sentiment_analysis=True)

@asset(description="Treina um modelo de aprendizado de máquina usando dados processados de economia. A função seleciona apenas as colunas numéricas do DataFrame para uso como variáveis independentes (X) e a coluna 'target' como a variável dependente (y). Divide os dados em conjuntos de treino e teste, treina um modelo RandomForest e avalia sua acurácia. Gera um gráfico de barras mostrando a acurácia do modelo.")
def treinar_ia_economia(tratar_dados_economia: pd.DataFrame) -> None:
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import accuracy_score
    logger = get_dagster_logger()

    # Prepara os dados para treinamento
    X = tratar_dados_economia.select_dtypes(include=[float, int])
    y = tratar_dados_economia['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Treina o modelo RandomForest
    modelo = RandomForestClassifier()
    modelo.fit(X_train, y_train)
    y_pred = modelo.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    
    # Gera e salva o gráfico de acurácia
    gerar_grafico_acuracia("Economia", accuracy)


@asset(description="Treina um modelo de aprendizado de máquina com dados de governo, incluindo a análise de sentimentos dos textos. Este processo inclui a preparação dos dados, onde as colunas numéricas são utilizadas como variáveis independentes e a coluna 'target' como dependente. Após dividir os dados em conjuntos de treino e teste, um modelo RandomForest é treinado e testado. A acurácia do modelo é avaliada e um gráfico de barras é gerado para visualizar a acurácia. Além disso, um gráfico de barras dos sentimentos das notícias é gerado para análise adicional.")
def treinar_ia_governo(tratar_dados_governo: pd.DataFrame) -> None:
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import accuracy_score
    logger = get_dagster_logger()

    # Prepara os dados para treinamento
    X = tratar_dados_governo.select_dtypes(include=[float, int])
    y = tratar_dados_governo['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Treina o modelo RandomForest
    modelo = RandomForestClassifier()
    modelo.fit(X_train, y_train)
    y_pred = modelo.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    
    # Gera e salva o gráfico de acurácia
    gerar_grafico_acuracia("Governo", accuracy)

    # Gera e salva o gráfico de sentimentos
    gerar_grafico_sentimentos(tratar_dados_governo, "Governo")

