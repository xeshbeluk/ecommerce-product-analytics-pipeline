from prefect import flow, task

@task
def say_hello(name: str) -> str:
    return f"Hello, {name}!"

@flow(name="test-flow")
def my_test_flow():
    result = say_hello("pipeline")
    print(result)

if __name__ == "__main__":
    my_test_flow()