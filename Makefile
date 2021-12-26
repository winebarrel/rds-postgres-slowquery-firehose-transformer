REPOSITORY_URL := 822997939312.dkr.ecr.ap-northeast-1.amazonaws.com/postgresql-slowquery

.PHONY: update
update: push
	aws lambda update-function-code --function-name postgresql-slowquery --image-uri $(REPOSITORY_URL):latest
	aws lambda wait function-updated --function-name postgresql-slowquery

.PHONY: image
image:
	docker build -t $(REPOSITORY_URL):latest .

.PHONY: push
push: image
	docker push $(REPOSITORY_URL):latest
