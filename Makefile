# Quick commands. RAW should point at extracted comma2k19 chunks (or Example_1).
PYTHON ?= python3
RAW ?= data/raw
DATA ?= data

.PHONY: synthetic ingest metrics dashboard test all demo-data

synthetic:            ## fake data in comma2k19 layout, with ground-truth events
	$(PYTHON) tools/make_synthetic.py --out data/synthetic/raw
	$(MAKE) RAW=data/synthetic/raw DATA=data/synthetic ingest metrics
	@echo "view with: make dashboard DATA=data/synthetic"

ingest:               ## raw numpy logs -> bronze Parquet
	$(PYTHON) pipeline/ingest.py --raw $(RAW) --out $(DATA)/bronze --workers 8

metrics:              ## bronze -> silver -> gold via Spark SQL (fails if DQ gate fails)
	$(PYTHON) pipeline/spark_job.py --bronze $(DATA)/bronze --out $(DATA) --strict

dashboard:
	$(PYTHON) -m streamlit run dashboard/app.py -- --data $(DATA)

test:
	$(PYTHON) -m pytest -q

all: ingest metrics dashboard

demo-data:            ## publish pipeline output to the deployed dashboard folder
	rm -rf demo_data/gold demo_data/silver
	cp -r $(DATA)/gold $(DATA)/silver demo_data/
	find demo_data -name '*.crc' -delete; find demo_data -name '_SUCCESS' -delete
	@echo "now update demo_data/source.json to describe the real data, then git push"

hf-demo:              ## REAL data, fast: 64 comma2k19 segments (228 MB) from the Hugging Face mirror
	mkdir -p data/hf
	for i in 0 1 2; do curl -L -C - -o data/hf/demo-0000$$i-of-00003.parquet \
	  "https://huggingface.co/datasets/commaai/comma2k19/resolve/main/data/demo-0000$$i-of-00003.parquet"; done
	$(PYTHON) tools/hf_demo_to_layout.py --parquet data/hf --out data/raw
	$(MAKE) ingest metrics
