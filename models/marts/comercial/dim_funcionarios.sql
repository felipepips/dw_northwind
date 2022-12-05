with
    funcionarios as (
        select *
        from {{ ref('stg_erp__funcionarios') }}
    )

    , self_joined as (
        select
            f.id_funcionario
            , f.nome_funcionario
            , f.sobrenome_funcionario
            , f.nome_completo_funcionario
            , f.cargo
            , f.titulo
            , f.data_nasc
            , f.data_contratacao
            , f.endereco
            , f.cidade
            , f.regiao
            , f.cep
            , f.pais
            , f.telefone
            , f.id_gerente
            , gerentes.nome_funcionario as nome_gerente
        from funcionarios f
        left join funcionarios as gerentes on
            f.id_gerente = gerentes.id_funcionario
    )

    , transformed as (
        select
            row_number() over (order by id_funcionario) as sk_funcionario
            , *
        from self_joined
    )

select *
from transformed