{%- macro pivot_for_loop(
        column_array,
        column, 
        then_column = 1, 
        else_column = 0, 
        agg='None', 
        comma = 'lead', 
        end_comma = 'no', 
        prefix = '',
        suffix = ''
) -%}

    {%- if comma == 'lead' -%}
        {%- set lead, trailing = ',', '' -%}
    {%- else -%}
        {%- set lead, trailing = '', ',' -%}
    {%- endif -%}

    {%- for i in column_array -%}
        {{lead}}
        {% if agg == 'None' -%}
            CASE WHEN {{ column }} = '{{ i }}' THEN {{ then_column }} ELSE {{ else_column }} END AS {{prefix}}{{ i }}{{suffix}}
        {%- else -%}
            {{ agg }}(CASE WHEN {{ column }} = '{{ i }}' THEN {{ then_column }} ELSE {{ else_column }} END) AS {{prefix}}{{ i }}{{suffix}}
        {%- endif -%}
        {%- if end_comma == 'no' and not loop.last -%}
            {{trailing}}
        {%- elif end_comma == 'yes'-%}
            {{trailing}}
        {%- endif -%}
        
    {%- endfor -%}

{%- endmacro -%}