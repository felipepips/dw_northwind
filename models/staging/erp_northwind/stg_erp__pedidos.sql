with
    source_orders as (
        select
            cast(order_id as int) as id_pedido
            , cast(customer_id as string) as id_cliente
            , cast(employee_id as int) as id_funcionario
            , cast(ship_via as int) as id_transportador
            , cast(order_date as date) as data_pedido
            , cast(required_date as date) as data_requerida
            , cast(shipped_date as date) as data_envio
            , cast(freight as numeric) as frete_pedido
            , cast(ship_name as string) as nome_destinatario
            , cast(ship_address as string) as endereco_destinatario
            , cast(ship_city as string) as cidade_destinatario
            , cast(ship_region as string) as regiao_destinatario
            , cast(ship_postal_code as string) as cep_destinatario
            , cast(ship_country as string) as pais_destinatario
            
        from {{ source('erp', 'orders') }}
    )

select *
from source_orders	