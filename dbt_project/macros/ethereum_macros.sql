
{% macro uint256_to_address(hex_string) %}
    '0x' || substring({{ hex_string }} from 27)
{% endmacro %}


{% macro uint256_to_numeric(hex_string) %}
    -- Convert hex string to numeric using SQL
    -- Handle uint256 values that exceed bigint limits
    case
        when {{ hex_string }} is null then null
        else (
            select sum(
                case
                    when digit = '0' then 0
                    when digit = '1' then 1 * power(16, pos)
                    when digit = '2' then 2 * power(16, pos)
                    when digit = '3' then 3 * power(16, pos)
                    when digit = '4' then 4 * power(16, pos)
                    when digit = '5' then 5 * power(16, pos)
                    when digit = '6' then 6 * power(16, pos)
                    when digit = '7' then 7 * power(16, pos)
                    when digit = '8' then 8 * power(16, pos)
                    when digit = '9' then 9 * power(16, pos)
                    when digit = 'a' then 10 * power(16, pos)
                    when digit = 'b' then 11 * power(16, pos)
                    when digit = 'c' then 12 * power(16, pos)
                    when digit = 'd' then 13 * power(16, pos)
                    when digit = 'e' then 14 * power(16, pos)
                    when digit = 'f' then 15 * power(16, pos)
                end
            )
            from (
                select
                    substring(lower(replace({{ hex_string }}, '0x', '')) from i for 1) as digit,
                    (length(lower(replace({{ hex_string }}, '0x', ''))) - i) as pos
                from generate_series(
                    length(lower(replace({{ hex_string }}, '0x', ''))),
                    1,
                    -1
                ) as i
            ) as digits
        )
    end
{% endmacro %}


{% macro convert_token_amount(amount_raw, decimals, output_precision=8) %}
    -- Convert raw token amount to decimal amount using actual token decimals
    -- Parameters:
    --   amount_raw: raw amount in smallest unit (e.g., wei or token's smallest unit)
    --   decimals: number of decimals for the token (from dim_stablecoin)
    --   output_precision: decimal precision for output (default 8)
    ({{ amount_raw }} / power(10, {{ decimals }}))::decimal(28, {{ output_precision }})
{% endmacro %}