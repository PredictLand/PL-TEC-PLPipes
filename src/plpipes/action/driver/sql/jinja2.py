import jinja2

def render_template(src, global_vars):
    env = jinja2.Environment()
    return env.from_string(src).render(**global_vars)
