with
    source_order_details as (
        select
            cast(order_id as int) as id_pedido
            , cast(product_id as int) as id_produto
            , cast(unit_price as numeric) as preco_unitario
            , cast(quantity as int) as quantidade
            , cast(discount as numeric) as desconto
        from {{ source('erp', 'order_details') }}
    )

select *
from source_order_details	