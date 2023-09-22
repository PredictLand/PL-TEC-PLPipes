# Introduction

[**PredictLand**](https://www.predictland.com/), the company behind
PLPipes, is a consultancy firm specializing in Data Science and
related fields (Data Analytics, AI and ML, Big Data, Data Engineering,
etc.). Our clientele spans from small businesses with only a handful
of employees to large corporations. This diversity demands flexibility
in our work methods because the platforms, IT systems, and tools at
our disposal can vary significantly from one project to another.

In fact, it's not uncommon for us to work on projects where our entire
infrastructure consists of nothing more than our laptops! Yes, you
read that correctly, no fancy environments like those provided for
instance by Databricks or Snowflake; no cloud instances with massive
amounts of RAM; no data automation services like Azure Data Factory or
DBT; sometimes not even a basic Database server. All we have are
our trusty laptops, a Git repository, and perhaps a few Excel files
containing the data.

So, we initiated `PLPipes` as an effort to replicate the capabilities
of those sophisticated frameworks suitable for our
resource-constrained environments. In this context, you can consider
PLPipes as a cost-effective Data Science framework, a cheap
alternative to solutions like Databricks and similar platforms!

However, nowadays, we prefer to view PLPipes as a **lean and highly
scalable framework**. It's a tool that you can use to train models
from a few CSVs on your laptop, process terabytes of data on a cloud
cluster, integrate into a lambda function, or run models within a
Docker container, and much more.

So, what is exactly PLPipes?

Several things:

1. It is a thin layer **integrating** several technologies so that
   they can be used easily and efficiently to solve common data
   science problems.

2. It is an **automation** framework for creating data processing
   pipelines.

3. It is a **programming** framework for reducing boilerplate,
   enforcing some best-practices and providing support for common
   tasks.

4. It is also a **mindset** and a way to **standardize** Data Science
   project development.

5. It is a very **customizable** framework with **sane defaults**, so
   that you can start working on your projects right there without
   having to perform a complex setup up front.

6. It is a **work in process** yet! Even if the ideas behind PLPipes
   are not new and we have used/implemented them in different forms
   and in different projects in the past (or in some cases, just
   copied them from other 3rd party projects), the framework is still
   very new and most of it should be considered experimental!


In any case, please note that `PLPipes` is not intended to replace
packages like `numpy`, `pandas`, `polars`, `dask`, `pyspark`,
`scikit-learn`, `tensorflow`, `pytorch`, `matplotlib`, `jupyter`, and
many other frameworks of that nature. You are expected to continue
using those libraries in your code, and PLPipes maintains a neutral
stance on which ones you choose to use.

Having say that, it is also true that some of those libraries
require some integration work not yet done... but well, this is a
young project, so, you can just ask for it or even better, get
involved in the project and contribute your patches!!!

