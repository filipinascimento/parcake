project(
  'parcake',
  version: '0.4.1',
  license: 'MIT',
  meson_version: '>=1.2'
)

python_mod = import('python')
py = python_mod.find_installation(pure: true)

# Explicit list keeps Meson compatible while ensuring all public modules ship.
package_sources = files(
  'src/parcake/__init__.py',
  'src/parcake/grouper.py',
  'src/parcake/reader.py',
  'src/parcake/saver.py',
  'src/parcake/sorter.py',
  'src/parcake/py.typed',
)

py.install_sources(package_sources, pure: true, subdir: 'parcake')
