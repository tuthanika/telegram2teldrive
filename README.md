# telegram2teldrive
Find existing files in Telegram channels that has not been uploaded using Teldrive, and add them to the Teldrive DB.

---

## TODO (perhaps)

- Use the Teldrive API instead of modifying DB directly.
- Add options to filter for certain files with support for regex. For example match all `r'.*\.mp[34]'` but not any of mime type *'image/jpeg'*.
- Add support for DB URI strings (postgresql://...)
- Add support for the reverse: removing files from the Teldrive database (but not from channel)
- Add support for redoing an import

## Prerequisites

- python3
- pip
- Telegram
  - API ID
  - API hash
  - associated phone number
- Teldrive
  - PostgreSQL database
    - name
    - username
    - password
    - host
    - port
- some files perhaps

## Installation

For Linux/MacOS:
```shell
git clone https://github.com/iwconfig/telegram2teldrive
cd telegram2teldrive
python3 -m venv pyvenv
source pyvenv/bin/activate
pip install -r requirements.txt
```
For Windows:
```shell
git clone https://github.com/iwconfig/telegram2teldrive
cd telegram2teldrive
python3 -m venv pyvenv
pyvenv/Scripts/activate
pip install -r requirements.txt
```

## Usage

> [!CAUTION]
> It is always good advice to make a backup of your database before you start.

> [!CAUTION]
> **Possible Unintended Consequences:** This goes without saying, but Teldrive manages these files the same as any other file uploaded through Teldrive once they are added to the Teldrive database. If you delete an imported file using the Teldrive Web UI or Rclone, it will eventually be permanently removed from the associated Telegram channel by the Teldrive cron job (a background maintenance program) after the time period specified by `--cronjobs-clean-files-interval`.
> 
> If you run this script and happen to need or want to redo the import for any reason, **do not** delete the files using Teldrive or Rclone and expect that they will be removed only from the database. A file deleted in Teldrive will be removed from the database *along with the source file in the Telegram channel.*
>
> **This is your responsibility.**

<br>

Get the Telegram API ID and hash from your teldrive `config.toml` file:

```toml
[tg]
app-id = "<Telegram API ID>"
app-hash = "<Telegram API hash>"
```

Get your DB name, username, password, host and port from either

```toml
[db]
data-source = "postgres://<db username>:<db password>@<db host>:<db port>/<db name>"
```

or from your docker compose file for your database (the `$POSTGRES_{USER,PASSWORD,DB}` environment variables):

```yaml
services:
  teldrive-db:
    image: groonga/pgroonga:latest-alpine-17-slim
    container_name: teldrive-db
    restart: unless-stopped
    security_opt:
      - apparmor=unconfined
    ports:
      - 5432:5432
    environment:
      - POSTGRES_USER=<db username>
      - POSTGRES_PASSWORD=<db password>
      - POSTGRES_DB=<db name>
    volumes:
      - ./teldrive/postgres_data:/var/lib/postgresql/data
```

If you use docker for your postgresql database, it is necessary to export 5432 like in the example above.

> [!CAUTION]
> It is always good advice to make a backup of your database before you start.


Now, either follow the help menu:

```shell
python3 telegram2teldrive.py --help
python3 telegram2teldrive.py --api-id 12345 --api-hash ... # etc
```

or add the following in the `telegram2teldrive.toml`:

```toml
[telegram]
api-id = "<Telegram API ID>"
api-hash = "<Telegram API hash>"
phone-number = "<phone number>" # The number bound to my telegram account in international format, i.e. +00123456789

[database]
name = "<db name>"
user = "<db user>"
password = "<db password>"

[teldrive]
folder_name = "<base dir>" # defaults to "Imported"
```

and then just do

```shell
python3 telegram2teldrive.py
```

or if config file is located somewhere else:

```shell
export CONFIG_FILE=/path/to/my_config_file.conf
python3 telegram2teldrive.py
# or
python3 telegram2teldrive.py --config /path/to/telegram2teldrive.toml
```

You'll now have go through the Telegram authentication process, though only once because the session is stored in the `./telegram2teldrive.session` file.

Then choose one or more channels and off it goes.

---

*All done, have a great day!*
