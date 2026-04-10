from pathlib import Path
import re
from typing import TextIO

import yaml
from brandizpyes.io import reader_helper
import os


def load_config ( yaml_source: Path|str|TextIO|None, use_unsafe_loader: bool = False ) -> dict:
	"""
	Loads a YAML configuration and returns it as a dictionary.

	This supports a simple syntax for environment variable resolution: `${ENV_VAR_NAME}`.

	## Parameters

	- `yaml_source`: the source of the YAML configuration. It can be a file path, a file-like object
	or a YAML string. If it's None, it will read from stdin. This uses :func:`brandizpyes.io.reader_helper`.

	- `use_unsafe_loader`: if true, uses :class:`yaml.UnsafeLoader` to load the configuration file. 
	This is useful when you want to do things like calling functions in the configuration file.
	However, it's False by default, since this behaviour is unsafe (see Python documentation).
	When false, it uses :class:`yaml.FullLoader`, which still allows for some dynamic features.

	TODO: move to brandizpyes (where it could be used for the logger).
	"""

	loader_cls = yaml.UnsafeLoader if use_unsafe_loader else yaml.FullLoader

	def reader ( fh: TextIO ) -> dict:
		# First read everything in a string
		yaml_str = fh.read ()

		# Then, replace the environment placeholders
		yaml_str = re.sub ( r"\$\{([^}]+)\}", lambda m: os.getenv ( m.group ( 1 ), "" ), yaml_str )

		# And finally, give it to the YAML loader
		return yaml.load ( yaml_str, Loader = loader_cls )

	return reader_helper ( reader, yaml_source )
