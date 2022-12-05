with
    pedidos as (
        select
            id_pedido
            , id_cliente
            , id_funcionario
            , id_transportador
            , data_pedido
            , data_requerida
            , data_envio
            , frete_pedido
            , nome_destinatario
            , endereco_destinatario
            , cidade_destinatario
            , regiao_destinatario
            , cep_destinatario
            , pais_destinatario
        from {{ ref('stg_erp__pedidos') }}
    )

    , pedidos_detalhes as (
        select 
            id_pedido
            , id_produto
            , preco_unitario
            , quantidade
            , desconto
        from {{ ref('stg_erp__pedidos_detalhes') }}
    )

    , joined as (
        select
            p.id_pedido
            , p.id_cliente
            , p.id_funcionario
            , p.id_transportador
            , pd.id_produto
            , pd.preco_unitario
            , pd.quantidade
            , pd.desconto
            , p.data_pedido
            , p.data_requerida
            , p.data_envio
            , p.frete_pedido
            , p.nome_destinatario
            , p.endereco_destinatario
            , p.cidade_destinatario
            , p.regiao_destinatario
            , p.cep_destinatario
            , p.pais_destinatario
        from pedidos p
        left join pedidos_detalhes pd on p.id_pedido = pd.id_pedido
    )

    select *
    from joined