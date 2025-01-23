# test_hello_world.py

import pytest
from src.rda_python_template.hello_world import get_string

def test_get_string():
   assert get_string('Bob') == 'Bob: Hello World!'

def test_raises_exception_on_non_string_arguments():
   with pytest.raises(TypeError):
      get_string(9)
