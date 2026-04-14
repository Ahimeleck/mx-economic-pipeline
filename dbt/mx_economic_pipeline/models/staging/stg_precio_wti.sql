with source as (
    select * from {{ source('raw', 'precio_wti') }}
),

deduplicado as (
    select
        fecha,
        precio_wti,
        fecha_carga,
        row_number() over (
            partition by fecha 
            order by fecha_carga desc
        ) as rn
    from source
)

select
    fecha,
    precio_wti,
    fecha_carga
from deduplicado
where rn = 1 
