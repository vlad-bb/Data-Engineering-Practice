include .env
export

.PHONY:  help tests clean pre-commit

help:
	@echo "Available commands:"
	@echo "  tests        - Run tests"
	@echo "  pre-commit  - Install pre-commit hooks"
	@echo "  clean       - Clean up cache files"
	

tests:
	source .venv/bin/activate && pytest job1/tests job2/tests -v

pre-commit:
	source .venv/bin/activate && pre-commit install
	source .venv/bin/activate && pre-commit run --all-files

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +

