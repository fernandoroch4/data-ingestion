# Data Ingestion
## Creating DATABASE
```sql
CREATE DATABASE bank;
```

## Creating TABLE
```sql
CREATE TABLE public.transactions (
	id uuid NOT NULL,
	source_account bigint NOT NULL,
	destination_account bigint NOT NULL,
	amount numeric NOT NULL,
	transaction_type varchar NOT NULL,
	"date" timestamp NOT NULL
);
```

## Generating FAKE data
```bash
# enabling virtualenv
$ source code/.venv/bin/activate

# installing requirements
$ pip install -r code/requirements.txt

# running generator
$ python code/generator.py
```