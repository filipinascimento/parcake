.PHONY: release sync-version commit tag wheel check-clean build docs docs-serve

PYTHON ?= python
TAG ?=
MSG ?= Release $(TAG)
TAG_MSG ?= Release $(TAG)

release: check-clean sync-version commit tag wheel

check-clean:
	@if ! git diff --quiet --cached || ! git diff --quiet; then 		echo 'Working tree must be clean before running release. Commit or stash changes first.'; 		git status --short; 		exit 1; 	fi

sync-version:
	@if [ -z "$(TAG)" ]; then 		echo 'Set TAG=vX.Y.Z (e.g. make release TAG=v0.2.0)'; 		exit 1; 	fi
	$(PYTHON) scripts/sync_version.py --version $(TAG)

commit:
	@if [ -z "$(TAG)" ]; then 		echo 'Set TAG=vX.Y.Z (e.g. make release TAG=v0.2.0)'; 		exit 1; 	fi
	@git add pyproject.toml meson.build
	@if git diff --cached --quiet; then 		echo 'No version changes staged; did sync-version run?'; 		exit 1; 	fi
	@git commit -m "$(MSG)"

tag:
	@if [ -z "$(TAG)" ]; then 		echo 'Set TAG=vX.Y.Z (e.g. make release TAG=v0.2.0)'; 		exit 1; 	fi
	@git tag -a $(TAG) -m "$(TAG_MSG)"

wheel:
	$(PYTHON) -m build

build: wheel


docs:
	$(PYTHON) -m mkdocs build

docs-serve:
	$(PYTHON) -m mkdocs serve
