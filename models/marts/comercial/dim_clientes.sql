with
    clientes as (
        select *
        from {{ ref('stg_erp__clientes') }}
    )

    , transformed as (
        select
            row_number() over (order by id_cliente) as sk_cliente
            , id_cliente
            , nome_cliente
            , nome_contato_cliente
            , titulo_contato_cliente
            , endereco_cliente
            , cidade_cliente
            , regiao_cliente
            , codigo_postal_cliente
            , pais_cliente
            , telefone_cliente
            , telefone2_cliente
        from clientes
    )

select *
from transformed