with
    source_shippers as (
        select
            cast(shipper_id as int) as id_transportador
            , cast(company_name as string) as nome_transportador
            , cast(phone as string) as telefone
        from {{ source('erp', 'shippers') }}
    )

select *
from source_shippers