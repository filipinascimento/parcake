project(
  'parcake',
  version: '0.4.0',
  license: 'MIT',
  meson_version: '>=1.2'
)

python_mod = import('python')
fs = import('fs')
py = python_mod.find_installation(pure: true)

# Collect every module in the parcel to avoid missing new utilities (like PieceGrouper).
package_sources = fs.glob('src/parcake/*.py')
package_sources += files('src/parcake/py.typed')

py.install_sources(package_sources, pure: true, subdir: 'parcake')
