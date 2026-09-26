# Storage

Backends and their configuration. Most applications only pass a config
dict to `CairnDB.configure`. See [Configuration](../configuration.md).

## Configuration

```{eval-rst}
.. autofunction:: cairndb.storage.create_storage

.. autoclass:: cairndb.storage.StorageConfig
   :no-show-inheritance:

.. autoclass:: cairndb.storage.FilesystemStorageConfig
   :exclude-members: create_storage

.. autoclass:: cairndb.storage.S3StorageConfig
   :exclude-members: create_storage

.. autoclass:: cairndb.storage.GCSStorageConfig
   :exclude-members: create_storage

.. autoclass:: cairndb.storage.AzureStorageConfig
   :exclude-members: create_storage
```

## Backend interface

```{eval-rst}
.. autoclass:: cairndb.storage.BlobStorage
   :no-show-inheritance:

.. autoclass:: cairndb.storage.StoredObject
   :no-show-inheritance:
```

## Backends

```{eval-rst}
.. autoclass:: cairndb.storage.filesystem.FilesystemStorage
   :members: __init__

.. autoclass:: cairndb.storage.s3.S3Storage
   :members: __init__

.. autoclass:: cairndb.storage.gcs.GCSStorage
   :members: __init__

.. autoclass:: cairndb.storage.azure.AzureBlobStorage
   :members: __init__

.. autoclass:: cairndb.engine.NamespacedStorage
   :members: __init__

.. autofunction:: cairndb.engine.log_storage

.. autodata:: cairndb.storage.base.DEFAULT_SCHEMA_VERSION
```
