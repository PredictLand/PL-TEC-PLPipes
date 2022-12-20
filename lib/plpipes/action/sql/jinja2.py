import jinja2

def render_template(src):
    env = jinja2.Environment()
    return env.from_string(src).render()
