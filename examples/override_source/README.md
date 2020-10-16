Minimalist example of overriding the behavior of the data source. This is useful in cases where data blocks are being stored in some other non-file form (e.g. an object store).

In this case we've overwritten the source to always return a file containing only 0x1's. 

Example build and run:
```
$ go build -o override_source main.go
$ chmod +x ./override_source
$ ./override_source
```

In a separate terminal:
```
$ hexdump -C /dev/nbd0
00000000  01 01 01 01 01 01 01 01  01 01 01 01 01 01 01 01  |................|
*
00100000
```
