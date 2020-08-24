# Base code using Task Flow Paradigm

Prefect (https://docs.prefect.io/) Task Flow paradigm base code for data science research

## Installing the requirements

``` 
pip install -r requirements.txt
pip install -r requirements-dev.txt
``` 
### Running the tests

Example test:

```
ENV_FOR_DYNACONF=test nosetests tests/tasks/sample_task/sample_task_test.py 
```

### Running the flows

Running the example flow:

```
python -m  sample.flows.sample_flow
```






