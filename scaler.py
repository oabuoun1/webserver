import time
from webserver import env_params
def start():
	for x in range(100):
		env_params.change(200)
		print(env_params)
		print(x)
		print("\n")
		time.sleep(3)