## EODAG download proxy

```
cargo run
```

### Cache only specific paths

When cache is enabled, you can restrict it to specific request-path prefixes:

```toml
[cache]
bucket = "my-cache"
endpoint = "https://s3.example.com"
region = "us-east-1"
access_key = "..."
secret_key = "..."
cache_path_prefixes = [
	"/data/xxx/yyy/",
	"/data/provider-a/collection-z/",
]
```

Behavior:
- `cache_path_prefixes = []` means cache all paths.
- Prefixes with or without a leading slash both work.
- Prefixes ending with `/` are treated as starts-with matches.
