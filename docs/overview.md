# Overview

`PLPipes` is a progresive framework in the sense that you are not
forced to use all of it in every project. You can just take advantage
of some of the subsystems offered and ignore the rest if that suits
you better.

## Subsystems

So, what are those subsystems?

* [Configuration](configuration.md): It handles the configuration of
    your project (e.g. database credentials, file patchs,
    hyperparameter, neural network definitions, etc.) and it is quite
    powerful, supporting many features that would help the developer
    keep everything tidy.

    It is the only mandatory component as all the rest build on top of
    it and use it pervasively.

* [Database](databases.md): It handles interaction with databases:
    connections, transactions, reading and writing tables, integration
    with data-frame frameworks as pandas or polars, etc.

* [Actions](actions.md): like scripts but better!

    This layer provides support for running several types of tasks as
    for instace running some python code, running a query in a database
    and saving the result as a new table, processing a `quarto`
    document, etc.

* [Runner](runner.md): It is a Python script which is used to run the
    actions taking care of setting up the configuration, parsing command
    line arguments, etc.

* [Cloud](cloud-intro.md): several packages are offered for calling into
    common Cloud APIs (Azure, GoogleCloud, AWS, OpenAI, etc.).

    Some of then, perform basic functionality as authentication,
    others offer more advanced features as a FS layer which and unified
    interface for accessing any supported storage service.


# The `PLPipes` mindset

Even if you can use some of those subsystems independently, in our
(quite biased) opinion, it is better when you use all of then
together!

The big gain is that all of your projects are going to look the
same. When somebody in your team gets to work in an already running
project, he will not have to ask how to configure the access to the
database, or how to get the data, or how to generate that fancy
monthly report because it is going to be the same or very similar to
its previous `PLPipes` projects.

So, how is the typical `PLPipes` project?

## The Actions

`PLPipes` projects are organized around [**actions**](#Actions) which
can be considered as atomic units of work. Examples of actions are
downloading a file, transforming some data or training a model.

Actions are grouped in sequences to create data processing
pipelines. Several pipelines can be defined inside one project, and it
is even possible to change which actions form a pipeline dynamically
depending on the deployment environment, the configuration, command
line arguments, etc.

Even if the framework doesn't impose it, actions are usually organized
in a similiar fashion. For instance, a common set of actions for a
simple project could be:

  - `download`
  - `preprocess`
  - `train`
  - `evaluate`
  - `report`

In a more complex project, those actions could become sequence actions
that call other actions doing smaller tasks, but the global structure
is going to remain alike.

## The Database

In the context of `PLPipes`, a central component is the relational
database, which serves as a means of exchanging information between
actions. While the file system and other means can be used
alternatively, the database is the preferred choice, at least tabular
data.

With the default configuration, `PLPipes` creates a SQLite database in
the local file system which is inmediately ready for the developer,
whitout any setup work or programming required from its side.

The framework also provides a rich set of functions for
common tasks, such as executing queries and reading the data as data
frames, appending data frames to tables, and synchronizing tables
between databases.

Utilizing a database offers several advantages:

1. Effortless Data Inspection: You can easily inspect the data using
   your preferred database GUI client
   ([SQLiteStudio](https://sqlitestudio.pl/),
   [DBeaver](https://dbeaver.io/), etc.), a Jupyter notebook, or
   simply the SQLite CLI. This allows you to explore the data, perform
   cross-referencing with other project data, and utilize SQL for
   queries.

2. Structured Data Design: Working with a database encourages
   thoughtful data modeling and design.

3. Schema Documentation: You can document the schema of your database,
   aiding in understanding and maintaining your data structure.

4. Clear Data Exchange: As you use the database to pass data between
   actions, you establish a well-defined interface, enhancing clarity
   and consistency.

This database-centric approach in `PLPipes` simplifies data management
and empowers you to work efficiently with your project's data
resources.

Finally, several local, remote and cloud Databases are
supported and can be configured.


## The Runner

The actions (or pipelines) are initiated by the [runner](runner.md),
which is essentially a Python script that interfaces with
`plpipes`. It has the capability to handle command-line arguments,
configuration files, and environment variables in a unified manner.

A typical runner invocation appears as follows:

```bash
python3 bin/run.py train evaluate -s model_name=resnet3
```

Users can also create custom runners to leverage the framework in
various environments, such as Azure FunctionApps, AWS Lambdas, Jupyter
notebooks, Spark and more.

## The Pervasive Configuration

All of `PLPipes` subsystems rely on a central configuration, each with
specific expectations for retrieving their data. For example,
configuring database connections consistently follows the same pattern
across any `PLPipes` project. Once you've configured it for one,
you'll find the process familiar and applicable to all.

While certain parameters may vary (configuring a connection to a local
file database differs from configuring one for a cloud-based server
like Azure SQL) the fundamental approach remains consistent at a
higher level.

This approach also simplifies resource tracking for individual
projects, as all resources are clearly declared within the
configuration files.

## The Helper Modules

Finally, `PLPipes` aims to be a rich framwork which could take care of
any task related to a data scientest work.

That is for instance the reason why it offers a cloud module and a
powerful package for accessing several cloud storage services. Because
even when it is not something core to the data scientist work, it is
something than in practice we frequently need to do, and so it goes in!

## In Summary

In summary, when using `PLPipes`, instead of a bunch of scripts, every
one doing something different, we have a set of pipelines built on top
of actions that use a relational database to store intermediate data
and we use a standardized python script to get everything running.

Additionally, it provides a lot of additional modules to make the data
scientist life much easier!
