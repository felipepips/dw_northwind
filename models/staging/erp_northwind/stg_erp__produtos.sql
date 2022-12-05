with
    source_products as (
        select
            cast(product_id as int) as id_produto
            , cast(product_name as string) as nome_produto
            , cast(supplier_id as int) as id_fornecedor
            , cast(category_id as int) as id_categoria
            , cast(quantity_per_unit as string) as qtd_por_unidade
            , cast(unit_price as numeric) as valor_unitario
            , cast(units_in_stock as int) as qtd_em_estoque
            , cast(units_on_order as int) as qtd_em_pedidos
            , cast(reorder_level as int) as nivel_recompra
            , case
                when discontinued = 1 then 'sim'
                else 'nao'
                end as descontinuado
        from {{ source('erp', 'products') }}
    )

select *
from source_products	