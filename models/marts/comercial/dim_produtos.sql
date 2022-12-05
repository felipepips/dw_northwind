with
    categorias as (
        select *
        from {{ ref('stg_erp__categorias')}}
    )

    , produtos as (
        select * 
        from {{ ref('stg_erp__produtos')}}
    )

    , fornecedores as (
        select * 
        from {{ ref('stg_erp__fornecedores')}}
    )        

    , uniao_tabelas as (
        select
            p.id_produto
            , p.id_fornecedor
            , p.id_categoria
            , p.nome_produto
            , p.qtd_por_unidade
            , p.valor_unitario
            , p.qtd_em_estoque
            , p.qtd_em_pedidos
            , p.nivel_recompra
            , c.nome_categoria
            , c.descricao_categoria
            , f.nome_fornecedor
            , f.nome_contato_fornecedor
            , f.titulo_contato_fornecedor
            , f.endereco_fornecedor
            , f.cidade_fornecedor
            , f.regiao_fornecedor
            , f.pais_fornecedor
            , f.codigo_postal_fornecedor
            , f.telefone_fornecedor
            , p.descontinuado
        from produtos p
        left join categorias c on p.id_categoria = c.id_categoria
        left join fornecedores f on p.id_fornecedor = f.id_fornecedor
    )

    , transformacoes as (
        select
            row_number() over (order by id_produto) as sk_produto
            , *
        from uniao_tabelas
    )

select *
from transformacoes