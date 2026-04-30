with source as (
    select * from {{ source('raw', 'product_catalog') }}
),

renamed as (
    select
        product_id,
        product_active               as active,
        product_sku                  as sku,
        product_vendorpart           as vendor_part,
        product_description          as description,
        product_category             as category,
        product_unitofmeasure        as unit_of_measure,
        product_listprice::float     as list_price,
        product_contractprice::float as contract_price,
        product_currency             as currency,
        product_leadtimedays::int    as lead_time_days,
        product_minorderqty::int     as min_order_qty,
        product_weight::float        as weight,
        product_weightunit           as weight_unit,
        _source_file                 as source_file,
        _ingested_at::timestamp      as ingested_at
    from source
)

select * from renamed