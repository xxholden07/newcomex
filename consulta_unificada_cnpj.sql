-- Consulta SQL unificada do CNPJ, trazendo os principais dados das tabelas e nomes de colunas amigáveis
SELECT
    e.cnpj_basico AS CNPJ_BASICO,
    e.razao_social AS RAZAO_SOCIAL,
    nj.descricao AS NATUREZA_JURIDICA,
    qr.descricao AS QUALIFICACAO_RESPONSAVEL,
    e.capital_social AS CAPITAL_SOCIAL,
    CASE e.porte_empresa
        WHEN '00' THEN 'NAO_INFORMADO'
        WHEN '01' THEN 'MICRO_EMPRESA'
        WHEN '03' THEN 'EPP'
        WHEN '05' THEN 'DEMAIS'
        ELSE e.porte_empresa
    END AS PORTE_EMPRESA,
    e.ente_federativo_responsavel AS ENTE_FEDERATIVO_RESPONSAVEL,
    est.cnpj_ordem AS CNPJ_ORDEM,
    est.cnpj_dv AS CNPJ_DV,
    est.identificador_matriz_filial AS IDENTIFICADOR_MATRIZ_FILIAL,
    est.nome_fantasia AS NOME_FANTASIA,
    est.situacao_cadastral AS SITUACAO_CADASTRAL,
    est.data_situacao_cadastral AS DATA_SITUACAO_CADASTRAL,
    mot.descricao AS MOTIVO_SITUACAO_CADASTRAL,
    est.nome_cidade_exterior AS NOME_CIDADE_EXTERIOR,
    p.descricao AS PAIS,
    est.data_inicio_atividade AS DATA_INICIO_ATIVIDADE,
    cnae.descricao AS CNAE_FISCAL_PRINCIPAL,
    est.cnae_fiscal_secundaria AS CNAE_FISCAL_SECUNDARIA,
    est.tipo_logradouro AS TIPO_LOGRADOURO,
    est.logradouro AS LOGRADOURO,
    est.numero AS NUMERO,
    est.complemento AS COMPLEMENTO,
    est.bairro AS BAIRRO,
    est.cep AS CEP,
    est.uf AS UF,
    m.descricao AS MUNICIPIO,
    est.ddd1 AS DDD1,
    est.telefone1 AS TELEFONE1,
    est.ddd2 AS DDD2,
    est.telefone2 AS TELEFONE2,
    est.ddd_fax AS DDD_FAX,
    est.fax AS FAX,
    est.email AS EMAIL,
    est.situacao_especial AS SITUACAO_ESPECIAL,
    est.data_situacao_especial AS DATA_SITUACAO_ESPECIAL,
    s.opcao_simples AS OPCAO_SIMPLES,
    s.data_opcao_simples AS DATA_OPCAO_SIMPLES,
    s.data_exclusao_simples AS DATA_EXCLUSAO_SIMPLES,
    s.opcao_mei AS OPCAO_MEI,
    s.data_opcao_mei AS DATA_OPCAO_MEI,
    s.data_exclusao_mei AS DATA_EXCLUSAO_MEI
FROM Empresas e
LEFT JOIN Naturezas nj ON e.natureza_juridica = nj.codigo
LEFT JOIN Qualificacoes qr ON e.qualificacao_responsavel = qr.codigo
LEFT JOIN Estabelecimentos est ON e.cnpj_basico = est.cnpj_basico
LEFT JOIN Motivos mot ON est.motivo_situacao_cadastral = mot.codigo
LEFT JOIN Paises p ON est.pais = p.codigo
LEFT JOIN Municipios m ON est.municipio = m.codigo
LEFT JOIN Cnaes cnae ON est.cnae_fiscal_principal = cnae.codigo
LEFT JOIN Simples s ON e.cnpj_basico = s.cnpj_basico;
