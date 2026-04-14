with source as (
    select * from {{ source('raw', 'tipo_cambio') }}
),

deduplicado as (
    select
        fecha,
        tipo_cambio,
        fecha_carga,
        row_number() over (
            partition by fecha 
            order by fecha_carga desc
        ) as rn
    from source
)

select
    fecha,
    tipo_cambio,
    fecha_carga
from deduplicado
where rn = 1 
