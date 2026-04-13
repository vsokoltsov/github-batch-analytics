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

sqlfluff:
	uv run sqlfluff lint -v dags/gba/services/dashboards

sqlfluff-fix:
	uv run sqlfluff fix dags/gba/services/dashboards

lint:
	make ty & make black-fix & make ruff & make sqlfluff

notebook:
	PYTHONPATH=. uv run jupyter lab
