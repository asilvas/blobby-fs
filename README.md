# blobby-fs

A FileSystem storage client for [Blobby](https://github.com/asilvas/blobby). 


## Options

```
# config/local.json5
{
  storage: {
    app: {
      options: {
        path: '/my/storage/path'
      }
    }
  }
}
```

| Option | Type | Default | Desc |
| --- | --- | --- | --- |
| path | string | (required) | File system path to store files |
