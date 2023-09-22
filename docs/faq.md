

# FAQ

## Design

* *Why is the database used to pass data between actions? Isn't that
    inefficient?*

    Usually it is not.

    Both SQLite and DuckDB are pretty fast reading and writing data so
    that the database trip is very rarely the bottleneck.

    Actually, if you are able to delegate the data transformation tasks
    to the database (writing SQL code or using some front-end as ibis),
    they would perform way faster than the equivalent pandas code.

    Coming back to the *why*. Using a database has several additional
    benefits:

    - It is quite easy to inspect intermediate data, just point your
        favorite SQL GUI (for instance, [DBeaver](https://dbeaver.io/))
        to the database and look at the tables you want to see.

    - It allows the programmer to easily add pre and post-condition
        checking scripts which unintrusively validate the data before and
        after every action is run (planed).

    - It allows one to switch between functional-equivalent actions
        easily. For instance, in order to add support for some new
        algorithm into a project, all that is required is to develop the
        new model-training action and to plug it into some pipeline.

    - It becomes easier for new people to get to work into the project,
        as they only need to understand the data in the tables where they
        are going to work.

    - It is easy to establish guidelines about documenting the
        intermediate information structure (something that never happens
        for in-process pipelines).

* *How should I break my program into actions?*

    Well, the truth is we are still learning about what are the best ways
    to structure data science projects around actions!

    Typically, there are three clear parts in a Data Science project:

    1. Data preprocessing
    2. Model training and validation
    3. Predicting

    Though, sometimes, it doesn't make sense to split the training and
    the prediction stages. For instance, when the model needs to be
    retrained every time as it happens with time series data.

    Then every one of the actions above may be broken in several
    sub-actions. For instance, as part of the preprocessing we would have
    a data-retrieving action (maybe composed of several sub-actions as
    well). And then two more actions for converting from bronze-quality
    data first to silver and then to gold (see the [Medallion
    architecture](https://www.databricks.com/glossary/medallion-architecture)).

    Then, inside the model training, we could have still some data
    manipulation actions in order to adapt the generic gold format to
    the format required by the specific model, then an action that
    trains and saves the model to disk and finally some action that
    calculates some KPIs.

    Otherwise, maybe for that particular algorithm it is easier to do
    the data preparation, training and evaluation in just one
    action.

    Note also, that `actions` are not the only available abstraction to
    be used with PLPipes. Code can be organized as regular Python
    modules inside the `lib` directory and called from multiple actions.

    In summary, Common sense should be applied. Actions should not be a
    straitjacket, but just another element in your tool-set!
