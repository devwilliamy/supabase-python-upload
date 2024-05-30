# Supabase Python Upload

This was created in order to upload a large CSV to Supabase.
I tried using a direct connetion to Supabase through psql but I had no idea what was being run or not and it also failed after 30 minutes on 2k records so I decided to just write a script to make it easier to see what was going on.

## Sample ENV

```
DB_HOST=
DB_PORT=
DB_NAME=
DB_USER=
DB_PASSWORD=
```

## How to run

1. Start your virtual env
2. Install requirements.txt 
3. python main.py