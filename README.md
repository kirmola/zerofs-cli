# ZeroFS Uploader

Upload files to ZeroFS from the command line.

## Setup

1. **Download the script:**
   ```bash
   wget [https://raw.githubusercontent.com/kirmola/zerofs-cli/main/zerofs.py](https://raw.githubusercontent.com/kirmola/zerofs-cli/main/zerofs.py)
   ```

2. **Create virtual environment (optional but recommended):**
   ```bash
   python3 -m venv env
   source env/bin/activate  # On Windows: env\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install requests tqdm
   ```

## Upload a File

```bash
python uploader.py upload myfile.zip \
  --bucket-code eu \
  --api-url https://zerofs.link/api/
```

## With Authentication

**Option 1:** Create `auth.json` in the same folder:
```json
{"token": "YOUR_TOKEN"}
```

**Option 2:** Pass token directly:
```bash
python uploader.py upload myfile.zip \
  --bucket-code eu \
  --api-url https://zerofs.link/api/ \
  --token YOUR_TOKEN
```

## Optional: Add a Note

```bash
python uploader.py upload myfile.zip \
  --bucket-code eu \
  --api-url https://zerofs.link/api/ \
  --note "some zip which isnt good for local"
```

That's it.