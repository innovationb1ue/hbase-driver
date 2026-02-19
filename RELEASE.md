Release & publishing

Steps to build and upload to TestPyPI (recommended):

1. Install build and twine if not present:

   python3 -m pip install --user build twine

2. Build distributions:

   rm -rf dist/*
   python3 -m build

3. Upload to TestPyPI using an API token (create token on https://test.pypi.org/manage/account/token/):

   export TWINE_USERNAME="__token__"
   export TWINE_PASSWORD="<your-token-here>"
   python3 -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*

4. After verification, upload to PyPI (production):

   python3 -m twine upload dist/*

Notes:
- The project version was bumped to 1.0.1 and 'protobuf' was added to project dependencies to ensure runtime for generated protobuf modules.
- Artifacts built by CI or locally appear in the dist/ directory; do not commit dist/ to git.