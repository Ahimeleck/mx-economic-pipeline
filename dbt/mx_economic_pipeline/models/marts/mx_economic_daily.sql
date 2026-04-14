with tipo_cambio as (
    select * from {{ ref('stg_tipo_cambio') }}
),

precio_wti as (
    select * from {{ ref('stg_precio_wti') }}
),

joined as (
    select
        tc.fecha,
        tc.tipo_cambio,
        wti.precio_wti,
        round(tc.tipo_cambio / wti.precio_wti, 6) as ratio_mxn_wti
    from tipo_cambio tc
    left join precio_wti wti
        on tc.fecha = wti.fecha
)

select * from joined 
