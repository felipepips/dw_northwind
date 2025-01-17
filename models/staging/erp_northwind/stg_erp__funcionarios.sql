with
    source_employees as (
        select
            cast(employee_id as int) as id_funcionario
            , cast(last_name as string) as nome_funcionario
            , cast(first_name as string) as sobrenome_funcionario
            , cast((first_name || " " || last_name) as string) as nome_completo_funcionario
            , cast(title as string) as cargo
            , cast(title_of_courtesy as string) as titulo
            , cast(birth_date as date) as data_nasc
            , cast(hire_date as date) as data_contratacao
            , cast(address as string) as endereco
            , cast(city as string) as cidade
            , cast(region as string) as regiao
            , cast(postal_code as string) as cep
            , cast(country as string) as pais
            , cast(home_phone as string) as telefone
            , cast(reports_to as int) as id_gerente
        from {{ source('erp', 'employees') }}
    )

select *
from source_employees