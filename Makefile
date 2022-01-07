REPOSITORY_URL := ************.dkr.ecr.ap-northeast-1.amazonaws.com/firehose-postgresql-slowquery

.PHONY: update
update: push
	aws lambda update-function-code --function-name firehose-postgresql-slowquery --image-uri $(REPOSITORY_URL):latest
	aws lambda wait function-updated --function-name firehose-postgresql-slowquery

.PHONY: image
image:
	docker build -t $(REPOSITORY_URL):latest .

.PHONY: push
push: image
	docker push $(REPOSITORY_URL):latest

.PHONY: tail
tail:
	aws logs tail --follow /aws/lambda/firehose-postgresql-slowquery
