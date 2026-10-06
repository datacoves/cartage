from cartage.init import credentials as creds


def names(c, *, required=None, secret=None):
    return [f.name for f in c.fields if (required is None or f.required == required) and (secret is None or f.secret == secret)]


def test_flat_fields_postgres():
    c = creds.credentials("postgres")
    assert set(names(c, required=True)) == {"host", "database", "username", "password"}
    assert names(c, secret=True) == ["password"]
    assert "drivername" not in names(c) and "query" not in names(c)
    assert not c.fallback and c.alternatives == ()


def test_alternatives_snowflake():
    c = creds.credentials("snowflake")
    assert {"host", "database"} <= set(names(c, required=True))
    assert {"password", "private_key"} <= set(c.alternatives)


def test_variants_bigquery():
    labels = creds.variants("bigquery")
    assert len(labels) == 2 and "Gcp Service Account" in labels
    oauth = creds.credentials("bigquery", variant=next(v for v in labels if "OAuth" in v))
    assert "client_secret" in names(oauth, secret=True)
    assert set(names(creds.credentials("bigquery"), required=True)) == {"project_id", "private_key", "client_email"}


def test_filesystem_picks_by_url_scheme():
    assert "aws_access_key_id" in names(creds.credentials("filesystem", url="s3://b/p"))
    assert "azure_storage_account_name" in names(creds.credentials("filesystem", url="az://c/p"))
    for url in ("./data", "/abs/path", "https://example.com/files"):
        local = creds.credentials("filesystem", url=url)
        assert local.fields == () and not local.fallback


def test_introspection_failure_falls_back(monkeypatch):
    def boom(_):
        raise RuntimeError("dlt changed")

    monkeypatch.setattr(creds, "_classes", boom)
    c = creds.credentials("snowflake")
    assert c.fallback and c.fields == ()
    assert c.docs == "https://dlthub.com/docs/dlt-ecosystem/destinations/snowflake"
    assert creds.variants("snowflake") == []


def test_local_databases_have_no_auth_alternatives():
    c = creds.credentials("duckdb")
    assert names(c, required=True) == [] and c.alternatives == ()


def test_factory_destinations_link_the_destinations_index():
    assert creds.credentials("sources.lake:factory").docs == "https://dlthub.com/docs/dlt-ecosystem/destinations/"


def test_variant_labels_are_unique():
    labels = creds.variants("filesystem")
    assert len(labels) == len(set(labels))


def test_companion_secrets_are_not_auth_alternatives():
    alternatives = creds.credentials("snowflake").alternatives
    assert "private_key" in alternatives and "private_key_passphrase" not in alternatives
