with
    transportadores as (
        select *
        from {{ ref('stg_erp__transportadores') }}
    )

    , transformed as (
        select
            row_number() over (order by id_transportador) as sk_transportador
            , id_transportador
            , nome_transportador
            , telefone
        from transportadores
    )

select *
from transformed