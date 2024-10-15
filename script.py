import sqlite3 as sqlite

# Lendo o banco de dados
conexao = sqlite.connect('dadosERP.db')

# Criando o cursor
cursor = conexao.cursor()

# Função pra criar as tabelas  
def criarTabelas():

    # Criação da tabela repres
    sql = """
        CREATE TABLE IF NOT EXISTS repres(
            CODPRESS INTEGER PRIMARY KEY,
            TIPOPESS STRING NOT NULL,
            NOMEFAN STRING NOT NULL,
            COMISSAOBASE NUMERIC NOT NULL
        )
    """
    cursor.execute(sql)

    # Criação da tabela produtos
    sql = """
        CREATE TABLE IF NOT EXISTS produtos(
            CODPROD INTEGER PRIMARY KEY,
            NOMEPROD STRING NOT NULL,
            CODFORNE INTEGER,
            UNIDADE INTEGER,
            ALIQICMS NUMERIC,   
            VALCUSTO NUMERIC,
            VALORVENDA NUMERIC,
            QTDMIN NUMERIC,
            QTDESTOQUE NUMERIC NOT NULL,
            GRUPO INTEGER NOT NULL,
            CLASSEESTOQ STRING NOT NULL,
            COMISSAO NUMERIC, 
            PESOBRUTO NUMERIC
        )    
    """
    cursor.execute(sql)

    # Criação da tabela fornClien
    sql ="""
        CREATE TABLE IF NOT EXISTS fornClien(
            CODCLIFOR INTEGER PRIMARY KEY,
            TIPOCF INTEGER NOT NULL,
            CODREPRES INTEGER,
            NOMEFAN STRING NOT NULL,
            CIDADE STRING,
            UF STRING,
            CODMUNICIPIO INTEGER,
            TIPOPESSOA INTEGER NOT NULL,
            COBRANC INTEGER NOT NULL,
            PRAZOPGTO
        )
    """
    cursor.execute(sql)
    
    # Criação da tabela pedidos
    sql = """
        CREATE TABLE IF NOT EXISTS pedidos(
            NUMPED INTEGER PRIMARY KEY,
            DATAPED DATE NOT NULL,
            HORAPED TIME NOT NULL,
            CODCLIEN INTEGER,
            ES VARCHAR(1) NOT NULL,
            FINALIDNFE INTEGER NOT NULL,
            SITUACAO INTEGER NOT NULL,
            PESO NUMERIC NOT NULL,
            PRAZOPGTO NUMERIC NOT NULL,
            VALORPRODS NUMERIC NOT NULL,
            VALORDESC NUMERIC NOT NULL,
            VALOR NUMERIC NOT NULL,
            VALBASEICMS NUMERIC NOT NULL,
            VALICMS NUMERIC NOT NULL,
            COMISSAO NUMERIC NOT NULL
            )
    """
    cursor.execute(sql)


    # Criação da tabela pedidosItem
    sql = """
        CREATE TABLE IF NOT EXISTS pedidosItem(
            NUMPED INTEGER PRIMARY KEY,
            NUMITEM INTEGER NOT NULL,
            CODPROD INTEGER NOT NULL,
            QTDE FLOAT NOT NULL, 
            VALUNIT FLOAT NOT NULL,
            UNID STRING,
            ALIQICMS FLOAT NOT NULL,
            COMISSAO FLOAT NOT NULL,
            STICMS INTEGER NOT NULL,
            CFOP INTEGER, 
            REDUCBASEICMS FLOAT NOT NULL
        )
    """
    cursor.execute(sql)


    conexao.commit()



def lerDadosRepres():
    # Criando o dicionário de dados
    dados = {}
    
    # SQL declarado fora do laço for para não ter que gastar processamento
    sql = """
        INSERT INTO repres(CODPRESS, TIPOPESS, NOMEFAN, COMISSAOBASE) VALUES(?, ?, ?, ?)
    """
    
    # Lendo o arquivo
    arquivoRepres = open('./Repres.csv', encoding='UTF-8')
    
    
    linha = arquivoRepres.readline().rstrip()
    # Ele não precisa da primeira linha porque não é dado, só é título
    linha = arquivoRepres.readline().strip()
    
    while linha != "":
        linha = linha.split(";")
        
        # Tratamento de excessão:
        # O python não entende , como float, precisa ser . 
        # # Pra isso, substituiremos no código

        # Lista funcionando, agora será inserido no dicionário auxiliar
        # Não será inserido o codpres, ele vai ser chave do dicionario
        dicAux = {
            "tipoPess": linha[1] if linha[1] else None,  # Tipo pessoa
            "nomeFan": linha[2] if linha[2] else None,   # Nome fantasia
            "comissaoBase": float(linha[3].replace(",",".")) if linha[3] else None  # Comissão base
            
        }
        
        # Inserindo no dicionário final (usando codpress como chave)
        dados[linha[0]] = dicAux
    
        linha = arquivoRepres.readline().rstrip()
    arquivoRepres.close()
    
    for chave, item in dados.items():
        
        try:
            cursor.execute(sql, (chave, dados[chave]["tipoPess"], dados[chave]["nomeFan"], dados[chave]["comissaoBase"], ))
        except sqlite.OperationalError as e:
            print(f"Erro: {e} ")
            return
    
    
    try:
        conexao.commit()
        print("Dados do arquivo Repres.csv gravados no banco")
    except Exception as e:
        print(f"Erro: {e}")
            
            
# O CSV de produtos está sendo separado por tab "\t"
def lerDadosProdutos():
    # Criando o dicionário de dados para produtos
    dadosProdutos = {}
    
    sql = """ 
    INSERT INTO produtos(CODPROD, NOMEPROD, CODFORNE, UNIDADE, 
                        ALIQICMS, VALCUSTO, VALORVENDA, QTDMIN, 
                        QTDESTOQUE, GRUPO, CLASSEESTOQ, COMISSAO, 
                        PESOBRUTO) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    arquivoProdutos = open('./Produtos.csv', encoding='UTF-8')
    linha = arquivoProdutos.readline().strip()
    # A primeira linha é título. Deve ser pulada 
    linha = arquivoProdutos.readline().strip()
    
    while linha != "":
        
        # Separando por tab
        # Problema do "\t": quando se separa por tab, caso a última coluna da tabela tenha valor vazio, 
        # ele entende como um tab e não salva
        linha = linha.split("\t")
        
        # Caso tenha vazio na última coluna, ele não pega. Por isso ele trata adicionando None (NULL no banco)
        if len(linha) <= 12:
            linha.append(None)

        # Cada linha será tratada. Caso ele entre e veja algum elemento como ";;", ou seja, vazio, ele atribui None também
        for i in range(len(linha)):
            if linha[i] == "":
                linha[i] = None
                


        # Todos os floats podem receber o replace no mesmo lugar ao invés de criar uma condicional
        # Todos os dados são convertidos somente na hora da inserção, pois precisam ser tratados antes, e os 
        # dados são tratados como String
        ## Caso especial: qtdMin tem um número "1,006.00" e precisa ser convertido pra 1006. Usando o replace().replace(), é possível fazer este tratamento mais facilmente (e caso haja mais de um caso, ele trata também)
        dicAux = {
            "nomeProd": linha[1],    # Nome do produto
            "codForne": int(linha[2]) if linha[2] else None,    # Código do fornecedor
            "unidade": int(linha[3]),     # Unidade
            "aliqICMS": float(linha[4].replace(",",".")) if linha[4] else None,   # Alíquota ICMS
            "valCusto": float(linha[5].replace(",",".")) if  linha[5] else None,  # Valor de custo
            "valorVenda": float(linha[6].replace(",",".")) if linha[6] else None, # Valor de venda
            "qtdMin": float(linha[7].replace(".","").replace(",",".")) if linha[7] else None,      # Quantidade mínima
            "qtdEstoque": float(linha[8].replace(",",".")) if linha[8] else None,  # Quantidade em estoque
            "grupo": int(linha[9]),       # Grupo
            "classeEstoq": linha[10],     # Classe de estoque
            "comissao": float(linha[11].replace(",",".")) if linha[11] else None, # Comissão
            "pesoBruto": float(linha[12].replace(",",".")) if linha[12] else None # Peso bruto
            }
        # Inserindo o dicionário auxiliar no dicionário final (usando codprod como chave)
        dadosProdutos[int(linha[0].replace(".",""))] = dicAux
        linha = arquivoProdutos.readline().strip()

    arquivoProdutos.close()

    # Listando todos os itens no dicionário de produtos 
    for chave, item in dadosProdutos.items():
        
        # Começando a inserção
        try:
            
            # Executando o código (a tupla termina com (..., ) para evitar SQL Injection)
            cursor.execute(sql, (chave, dadosProdutos[chave]["nomeProd"], dadosProdutos[chave]["codForne"], 
                                 dadosProdutos[chave]["unidade"],dadosProdutos[chave]["aliqICMS"], 
                                 dadosProdutos[chave]["valCusto"], dadosProdutos[chave]["valorVenda"], 
                                 dadosProdutos[chave]["qtdMin"], dadosProdutos[chave]["qtdEstoque"],
                                 dadosProdutos[chave]["grupo"], dadosProdutos[chave]["classeEstoq"],
                                 dadosProdutos[chave]["comissao"], dadosProdutos[chave]["pesoBruto"], ))
        except sqlite.OperationalError as e:
            print(f"Erro: {e}")
            return


    # Salvando as mudanças - sempre fora do laço pra commitar tudo de uma vez
    try:
        conexao.commit()
        print("Dados do arquivo Produtos.csv gravados no banco")
    except Exception as e:
        print(f"Erro: {e}")
    


def lerDadosFornClien():
    
    # Lendo o arquivo
    arquivoFornClien = open('./FornClien.csv', encoding='UTF-8')


    sql = """
            INSERT INTO fornClien(CODCLIFOR, TIPOCF, CODREPRES, NOMEFAN,CIDADE, UF, CODMUNICIPIO, TIPOPESSOA, COBRANC, PRAZOPGTO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    dados = {}
    
    # Jogando a primeira linha fora
    linha = arquivoFornClien.readline().strip()
    linha = arquivoFornClien.readline().strip()
    
    while linha != "":
        
        linha = linha.split(";")
        
        for i in range(len(linha)):
            
            if linha[i] == "":
                linha[i] = None
                
        dicAux = {
            "tipoCf": int(linha[1]), # tipoCf
            "codRepres": int(linha[2]) if linha[2] else None, # codRepres
            "nomeFan": linha[3], # nomeFan
            "cidade": linha[4] if linha[4] else None, # cidade
            "uf": linha[5] if linha[5] else None, # uf
            "codMunicipio": int(linha[6]) if linha[6] else None, # codMunicipio
            "tipoPessoa": int(linha[7]) if linha[7] else None, # tipoPessoa
            "cobranc": int(linha[8]) if linha[8] else None, # cobranc
            "prazoPgto": int(linha[9]) if linha[9] else None # prazoPgto
        }
        
        dados[int(linha[0].replace(".",""))] = dicAux
        linha = arquivoFornClien.readline().strip()

    arquivoFornClien.close()

    for chave, item in dados.items():

        try:
            
            cursor.execute(sql, (chave, dados[chave]["tipoCf"], dados[chave]["codRepres"], dados[chave]["nomeFan"],
                                dados[chave]["cidade"], dados[chave]["uf"], dados[chave]["codMunicipio"],
                                dados[chave]["tipoPessoa"], dados[chave]["cobranc"], dados[chave]["prazoPgto"],))

        except sqlite.OperationalError as e:
            print(f"Erro: {e}")
            return
   
    try:
        conexao.commit()
        print("Dados do arquivo FornClien.csv gravados no banco")
    except Exception as e:
        print(f"Erro: {e}")
        
        
def lerDadosPedidos():

    dados = {}
    
    sql = """
        INSERT INTO pedidos(NUMPED, DATAPED, HORAPED, CODCLIEN, ES, FINALIDNFE, SITUACAO, PESO, PRAZOPGTO, VALORPRODS, VALORDESC, VALOR, VALBASEICMS, VALICMS, COMISSAO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""
    
    arquivoPedidos = open('./Pedidos.csv')
    
    linha = arquivoPedidos.readline().strip()
    # Descartando a linha de título
    linha = arquivoPedidos.readline().strip()
    
    while linha != "":
        linha = linha.split(";")
        
        
        # Tratamento para linhas vazias
        for i in range(len(linha)):
            if linha[i] == "":
                linha[i] = None
            
        # ----> Formatações <---- 
        #  2ª forma de fazer (a primeira forma - feita nos primeiros códigos - é mais enxuta, tem menos código, mas pode ficar um pouco bagunçado demais) 
        
        # NumPed
        linha[0] = int(linha[0].replace(".",""))

        # CodClien - condicional no final porque há valor nulo, e não se deve converter quando é nulo
        linha[3] = int(linha[3].replace(".","")) if linha[3] else None
        
        # finalidNfe, situacao
        linha[5] = int(linha[5])
        linha[6] = int(linha[6])
        
        # peso - arredonddado para 3 casas decimais somente
        linha[7] = float(linha[7].replace(".","").replace(",","."))
                
        # prazoPgto
        linha[8] = int(linha[8])
        
        # valorProds 
        linha[9] = float(linha[9].replace(".","").replace(",","."))
        
        # valorDesc
        linha[10] = float(linha[10].replace(",",""))
        
        # valor 
        linha[11] = float(linha[11].replace(".","").replace(",",".")) 

        # valBaseICMS
        linha[12] = float(linha[12].replace(".","").replace(",","."))
        
        # valICMS 
        linha[13] = float(linha[13].replace(".","").replace(",","."))
        
        # comissao
        linha[14] = float(linha[14].replace(",","."))
        
        
        # Data - salvando no padrão YYYY/mm/dd - só inverter os índices da data (não é necessário, o SQLITE pode salvar em dd/mm/YYYY):
        # Duas formas: usando três variáveis, ou uma lista
        data = []
        data = linha[1].split(".")
        linha[1] = f"{data[0]}/{data[1]}/{data[2]}"
        
        dicAux = {
            "dataPed": linha[1],
            "horaPed": linha[2],
            "codClien": linha[3],
            "es": linha[4],
            "finalidNfe": linha[5],
            "situacao": linha[6],
            "peso": linha[7],
            "prazoPgto": linha[8],
            "valorProds": linha[9],
            "valorDesc": linha[10],
            "valor": linha[11],
            "valBaseICMS": linha[12],
            "valICMS": linha[13],
            "comissao": linha[14]
            }
        
        dados[linha[0]] = dicAux
        linha = arquivoPedidos.readline().strip()
    arquivoPedidos.close()

    for chave, item in dados.items():
        try:
            cursor.execute(sql, (chave, dados[chave]["dataPed"], dados[chave]["horaPed"], 
                                 dados[chave]["codClien"], dados[chave]["es"],
                                 dados[chave]["finalidNfe"], dados[chave]["situacao"],
                                 dados[chave]["peso"], dados[chave]["prazoPgto"],
                                 dados[chave]["valorProds"], dados[chave]["valorDesc"],
                                 dados[chave]["valor"], dados[chave]["valBaseICMS"],
                                 dados[chave]["valICMS"], dados[chave]["comissao"],))

        except sqlite.OperationalError as e:
            print(f"Erro: {e}")
            return
    
    try:
        conexao.commit()
        print("Dados do arquivo Pedidos.csv gravados no banco")
    except Exception as e:
        print(f"Erro: {e}")

    
def lerDadosPedidosItem():

    arquivoPedidosItem = open('./PedidosItem.csv', encoding='UTF-8')
    dados = {}

    sql = """
        INSERT INTO pedidosItem (
        NUMPED, NUMITEM, CODPROD,
        QTDE, VALUNIT, UNID,
        ALIQICMS, COMISSAO, STICMS,
        CFOP, REDUCBASEICMS
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """


    linha = arquivoPedidosItem.readline().strip()
    linha = arquivoPedidosItem.readline().strip()

    while linha != "":
        linha = linha.split(';')

    

        # Tendo elemento vazio, preenche com None
        for i in range(len(linha)):
            if linha[i] == "":
                linha[i] = None
        
        # Convertendo os dados
        linha[0] = int(linha[0].replace(".",""))        # numPed
        linha[1]= int(linha[1])                         # numItem
        linha[2] = int(linha[2].replace(".",""))        # codProd
        linha[3] = float(linha[3].replace(",","."))     # qtde
        linha[4] = float(linha[4].replace(".","").replace(",","."))     # valUnit
        linha[5] = linha[5] if linha[5] else None       # unid
        linha[6] = float(linha[6].replace(",","."))     # aliqICMS
        linha[7] = float(linha[7].replace(",","."))     # comissao
        linha[8] = int(linha[8])                        # stICMS
        linha[9] = int(linha[9]) if linha[9] else None  # cfop
        linha[10] = float(linha[10].replace(",","."))   # reducBaseICMS

        dicAux = {
            "numItem": linha[1],
            "codProd": linha[2],
            "qtde": linha[3],
            "valUnit": linha[4],
            "unid": linha[5],
            "aliqICMS": linha[6],
            "comissao": linha[7],
            "stICMS": linha[8],
            "cfop": linha[9],
            "reducBaseICMS": linha[10]
        }

        dados[linha[0]] = dicAux 

        linha = arquivoPedidosItem.readline().strip()
    arquivoPedidosItem.close()
    for chave, item in dados.items():
        try:
            cursor.execute(sql,(chave, dicAux["numItem"], dicAux["codProd"],
                            dicAux["qtde"], dicAux["valUnit"], dicAux["unid"],
                            dicAux["aliqICMS"], dicAux["comissao"], dicAux["stICMS"],
                            dicAux["cfop"], dicAux["reducBaseICMS"]))
        except sqlite.OperationalError as e:
            print(f"Erro na inserção: {e}")
    
    try:
        conexao.commit()
        print("Dados do arquivo PedidosItem.csv gravados com sucesso")
    except Exception as e:
        print("Erro na gravação de Pedidos Item")


# COMO FUNCIONA O ARQUIVO PEDIDOSITEM - PERGUNTAR PRO BANIM


# Executando as funções através de uma Main
def main():
    try:  
        criarTabelas()
    
    # Verificando se o banco existe. Existindo, pula a execução e não retorna erro
    except sqlite.OperationalError:
        pass
    
    lerDadosRepres()
    lerDadosProdutos()
    lerDadosFornClien()
    lerDadosPedidos()
    lerDadosPedidosItem()
    conexao.close()


main()
