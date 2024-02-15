rm -rf ./dist/*
python3 -m build
yes | pip uninstall hbase-driver
pip install dist/hbase_driver-*-py3-none-any.whl
python3 -m twine upload --repository pypi dist/*
