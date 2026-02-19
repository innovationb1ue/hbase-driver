Dev environment: docker-compose-based HBase and Python dev container

To start everything:

  docker compose up --build

This starts Zookeeper, HBase, and a 'dev' container (hbase_dev) that mounts the repo and installs the Python package in editable mode.
Enter the dev container to run tests or examples:

  docker exec -it hbase_dev bash

Inside the container you can run examples from the README or a quick import test:

  python -c "from hbasedriver.client import Client; print('imported OK')"
