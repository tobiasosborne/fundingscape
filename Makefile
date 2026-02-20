.PHONY: init test update report clean

init:
	uv sync --all-extras
	uv run python -c "from fundingscape.db import init_db; init_db()"

test:
	uv run pytest -v

update:
	uv run python -m fundingscape.update

report:
	uv run python -m fundingscape.report

clean:
	rm -rf data/db/*.duckdb data/cache/*
