.PHONY: docs


docker-image:
	docker build -t stockprophet . --no-cache
