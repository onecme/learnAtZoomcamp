{% macro get_payment_type_description(payment_type) %}

case
    when {{ payment_type }} = 1 then 'Credit Card'
    when {{ payment_type }} = 2 then 'Cash'
    when {{ payment_type }} = 3 then 'No Charge'
    when {{ payment_type }} = 4 then 'Dispute'
    when {{ payment_type }} = 5 then 'Unknown'
    when {{ payment_type }} = 6 then 'Voided Trip'
    else 'Unknown'
end

{% endmacro %}
