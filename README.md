cbfs-fuse
=========

```
cbfs-fuse [-mnt=/mnt/cbfs] [-root=http://cbfs:8484/]

Flags:
  -mnt="/mnt/cbfs"            mount point
  -root="http://cbfs:8484/"   cbfs root url
```

Caveats
-------

* Go's `encoding/json` incorrectly handles integers above 999999, causing any
  directory with more than that many bytes in it inaccessible via cbfs-fuse.
* I haven't figured out a good way to allow write access to the filesystem. If
  you're really brave, you can change the constant at the top of main.go, but
  your files will probably get corrupted.
* The executable bit is never set on files, so you need to copy executables to
  a real filesystem before running them.
