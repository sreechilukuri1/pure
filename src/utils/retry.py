from tenacity import retry, stop_after_attempt, wait_exponential

def default_retry():
    return retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2)
    )