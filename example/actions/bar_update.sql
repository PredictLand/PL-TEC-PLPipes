
{% for i in range(0,10) %}

     insert into bar (foo) values ({{ i }});

{% endfor %}
