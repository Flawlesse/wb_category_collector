build:
	docker build -t wb_cat_collector .
run:
	export DB_DIR=$$(pwd)/data; \
	export LOG_DIR=$$(pwd)/log; \
	docker run --rm -v $$DB_DIR:/data -v $$LOG_DIR:/logs \
    	-e DB_PATH=wb.db -e LOG_PATH=app.log \
  		wb_cat_collector
clear:
	export DB_DIR=$$(pwd)/data; \
	export LOG_DIR=$$(pwd)/log; \
	sudo rm -rf $$DB_DIR $$LOG_DIR
