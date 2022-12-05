with
    clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )

    , funcionarios as (
        select *
        from {{ ref('dim_funcionarios') }}
    )

    , produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )

    , transportadores as (
        select *
        from {{ ref('dim_transportadores') }}
    )

    , pedidos_detalhes as (
        select *
        from {{ ref('int_vendas__pedidos_detalhes') }}
    )

    , joined as (
        select
            pd.id_pedido
            , f.sk_funcionario as fk_funcionario
            , c.sk_cliente as fk_cliente
            , t.sk_transportador as fk_transportador
            , p.sk_produto as fk_produto
            , pd.desconto
            , pd.preco_unitario
            , pd.quantidade
            , pd.frete_pedido
            , pd.data_pedido
            , pd.data_envio
            , pd.data_requerida
            , pd.nome_destinatario
            , pd.endereco_destinatario
            , pd.cep_destinatario
            , pd.cidade_destinatario
            , pd.regiao_destinatario
            , pd.pais_destinatario
            , c.nome_cliente
            , f.nome_completo_funcionario
            , f.nome_gerente
            , t.nome_transportador
            , p.nome_produto
            , p.nome_categoria
            , p.nome_fornecedor
            , p.descontinuado
        from pedidos_detalhes pd
        left join clientes c on pd.id_cliente = c.id_cliente
        left join funcionarios f on pd.id_funcionario = f.id_funcionario
        left join produtos p on pd.id_produto = p.id_produto
        left join transportadores t on pd.id_transportador = t.id_transportador
    )

    , transformacoes as (
        select
            {{ dbt_utils.surrogate_key(['id_pedido', 'fk_produto']) }} as sk_vendas
            ,*
            , case
                when desconto > 0 then "sim"
                when desconto = 0 then "nao"
                else "nao"
                end as teve_desconto
            , preco_unitario * quantidade as total_bruto
            , (1-desconto) * preco_unitario * quantidade as total_liquido

        from joined
    )

select *
from transformacoes