project(
  'parcake',
  version: '0.1.3',
  license: 'MIT',
  meson_version: '>=1.2'
)

python_mod = import('python')
py = python_mod.find_installation(pure: true)

package_sources = files(
  'src/parcake/__init__.py',
  'src/parcake/saver.py',
  'src/parcake/reader.py',
  'src/parcake/py.typed',
)

py.install_sources(package_sources, pure: true, subdir: 'parcake')
