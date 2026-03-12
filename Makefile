install-kernel:
	uv run python -m ipykernel install --user --name=github-batch-analytics --display-name="GitHub Batch Analytics"

ty:
	uv run ty check dags/ tests/

black:
	uv run black --check dags/ tests/

black-fix:
	uv run black dags/ tests/

ruff:
	uv run ruff check dags/ tests/ --fix

lint:
	make ty & make black-fix & make ruff

notebook:
	PYTHONPATH=. uv run jupyter lab
