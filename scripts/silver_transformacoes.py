import os
import pandas as pd
import json
import gcsfs

def processar_dados_gcs(bucket_name, chave_json, formato_entrada, caminho_particionado,
                        limpar_colunas, pasta_saida_local, grupos, particionado=False):
    try:
        # Autentica no GCS
        with open(chave_json) as f:
            credenciais = json.load(f)
        fs = gcsfs.GCSFileSystem(token=credenciais)
        print(fs.ls(bucket_name))
        # Lista arquivos no bucket
        if particionado:
            arquivos = fs.glob(f'{bucket_name}/{caminho_particionado}*.{formato_entrada}')
        else:
            arquivos = [f'{bucket_name}/{caminho_particionado}.{formato_entrada}']

        for caminho_completo in arquivos:
            print(f'Processando {caminho_completo}...')
            nome_base = os.path.splitext(os.path.basename(caminho_completo))[0]

            # Lê o Parquet diretamente do GCS
            with fs.open(caminho_completo, 'rb') as f:
                df = pd.read_parquet(f)

            # Remove colunas que o usuário quer limpar
            for col in limpar_colunas:
                if col in df.columns:
                    df.drop(columns=[col], inplace=True)

            # Para cada grupo de colunas, cria um arquivo separado
            for grupo, colunas in grupos.items():
                colunas_existentes = [col for col in colunas if col in df.columns]
                if not colunas_existentes:
                    continue

                df_subset = df[colunas_existentes]

                caminho_saida = f"{bucket_name}/{pasta_saida_local}/{grupo}/{nome_base}_{grupo}.parquet"
                print(f'Salvando arquivo {caminho_saida}...')

                # Salva diretamente no GCS
                with fs.open(caminho_saida, 'wb') as f_out:
                    df_subset.to_parquet(f_out, index=False)

        print("Processamento finalizado.")
    except Exception as e: 
        print(f"Ocorreu um erro: {e}")

if __name__ == "__main__":
    # Configurações
    bucket_name = 'enem-bucket-pedrorgc-raipb'
    chave_json = 'C:/Users/Ronnie/OneDrive/Documentos/Estudos/ADS/BD2/projeto-enem/EngenhariaDadosEnem-main/chave/enem-ifnmg-pedrorgc-raipb-da426803e45c.json'

    # Parâmetros para dados particionados
    formato_entrada = 'parquet'
    caminho_particionado = 'bronze/parquet/MICRODADOS_ENEM_2023_chunk_'
    
    #******************Transformaçoes*********************************
    # Adicione suas transformacoes aqui
    remover_colunas = [
        # Códigos das provas
        'CO_PROVA_CN', 'CO_PROVA_CH', 'CO_PROVA_LC', 'CO_PROVA_MT',
        # Respostas e gabaritos
        'TX_RESPOSTAS_CN', 'TX_RESPOSTAS_CH', 'TX_RESPOSTAS_LC', 'TX_RESPOSTAS_MT',
        'TX_GABARITO_CN', 'TX_GABARITO_CH', 'TX_GABARITO_LC', 'TX_GABARITO_MT',
        'TX_RESPOSTA_REDACAO',
        # Detalhes internos da redação
        'NU_NOTA_COMP1', 'NU_NOTA_COMP2', 'NU_NOTA_COMP3', 'NU_NOTA_COMP4', 'NU_NOTA_COMP5',
        # Necessidades específicas (remover se não for analisar acessibilidade)
        'IN_BAIXA_VISAO', 'IN_CEGUEIRA', 'IN_DEF_AUDITIVA', 'IN_DEF_FISICA',
        'IN_SURDEZ', 'IN_DISLEXIA', 'IN_DISCALCULIA', 'IN_AUTISMO',
        'IN_SABATISTA', 'IN_GESTANTE', 'IN_IDOSO', 'IN_VESTIBULAR', 'IN_TREINEIRO',
        # Dados administrativos
        'TP_SITUACAO', 'TP_LOCAL_PROVA'
    ]

    # Definição dos grupos de dados relevantes
    grupos_dados = {
        "participante": [
            "NU_INSCRICAO", "NU_ANO", "TP_FAIXA_ETARIA", "NU_IDADE", "TP_SEXO", "TP_COR_RACA",
            "CO_MUNICIPIO_RESIDENCIA", "NO_MUNICIPIO_RESIDENCIA", "CO_UF_RESIDENCIA", "SG_UF_RESIDENCIA"
        ],
        "escola": [
            "NU_INSCRICAO", "TP_ESCOLA", "TP_ENSINO", "TP_DEPENDENCIA_ADM_ESC",
            "CO_MUNICIPIO_ESC", "NO_MUNICIPIO_ESC", "CO_UF_ESC", "SG_UF_ESC"
        ],
        "notas": [
            "NU_INSCRICAO", "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO",
            "TP_STATUS_REDACAO"
        ],
        "presenca": [
            "NU_INSCRICAO", "TP_PRESENCA_CN", "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT"
        ],
        "socioeconomico": [
            "NU_INSCRICAO", "Q001", "Q002", "Q006", "Q024", "Q025", "Q026"
        ]
    }

    # Executa o processamento
    processar_dados_gcs(
        bucket_name=bucket_name,
        chave_json=chave_json,
        formato_entrada=formato_entrada,
        caminho_particionado=caminho_particionado,
        limpar_colunas=remover_colunas,
        pasta_saida_local='silver/parquet',
        grupos=grupos_dados,
        particionado=True
    )
