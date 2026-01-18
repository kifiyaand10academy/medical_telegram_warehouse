from dotenv import load_dotenv
import os
import subprocess

load_dotenv()  # loads .env file into environment

subprocess.run(["dbt"] + os.sys.argv[1:])
