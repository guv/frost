# frost

**frost** is a library for binary serialization of Clojure data structures.
It is built on top of the [kryo](http://github.com/EsotericSoftware/kryo) library.
The main goal of the library is to provide an easy method to binary serialization of Clojure data to byte arrays and files.
The general usage paradigm of the library is "*write once, read often*", i.e. there are no functions to modify any previously written file.

## Project Maturity

This library has been used in internal projects for more than a year. API changes are not very likely (except for additions).

## Install

Add the following to your dependency vector in your project.clj:

```clojure
[frost "0.5.0"]
```

Latest on [clojars.org](http://clojars.org):

![Version](https://clojars.org/frost/latest-version.svg)

## Quickstart

The namespace ```frost.quick-freeze``` provides functions to write given data to a byte array in memory or a file on the filesystem
and to read the data back again.
```clojure
(require '[frost.quick-freeze :as qf])
```

### Writing to and reading from files

The following code shows how a vector of maps can be written to a file and be read back again.
```clojure
(let [ms [{:a 1}, {:b 2}, {:c 3}]]
  (qf/quick-file-freeze "test.data" ms))
; => nil

(qf/quick-file-defrost "test.data")
; => [{:a 1} {:b 2} {:c 3}]
```
In the above case the whole vector is stored as one object which in some cases might not be desired for later processing of the file content.
In case you want to be able to read the objects in the vector iteratively you can do the following.
```clojure
(let [ms [{:a 1}, {:b 2}, {:c 3}]]
  (qf/quick-file-freeze-coll "test-coll.data" ms))
; => nil

(qf/quick-file-defrost "test-coll.data")
; => {:a 1}
```
Since the elements of the vector are stored separately ```quick-file-defrost``` returns only the first element of the vector.
To get the whole collection that is stored in the file the function ```quick-file-defrost-coll``` needs to be used.
```clojure
(qf/quick-file-defrost-coll "test-coll.data")
; => [{:a 1} {:b 2} {:c 3}]
```
What is the advantage of storing the elements of the vector individually in the file?
For example not the whole vector must be read back into memory -- instead a given predicate function can be used to specify which elements
are needed in the result vector.
```clojure
(qf/quick-file-defrost-coll "test-coll.data" :filter-pred #(some odd? (vals %)))
; => [{:a 1} {:c 3}]
```
The above code only includes those maps that contain at least one odd value.
Functions can also be applied to the read elements so that only the function value is stored.
The following example determines only the keys of each map that fulfills the predicate.
```clojure
(qf/quick-file-defrost-coll "test-coll.data" :filter-pred #(some odd? (vals %)) :process-fn keys)
; => [(:a) (:c)]
```

### File header

The created files contain a header that contains additional information about the file.
```clojure
(qf/quick-file-header "test-coll.data")
; => {:frost-version "0.4.0", :persistent-metadata true, :default-serializers true, :registration-required true,
;     :compression-algorithm :snappy, :no-wrap true, :compressed true}
```
That header can also contain application specific information about the file.
```clojure
(let [ms [{:a 1}, {:b 2}, {:c 3}]]
  (qf/quick-file-freeze-coll "test-coll-info.data" ms :file-info {:abc-test-data true}))
; => nil

(qf/quick-file-info "test-coll-info.data")
; => {:abc-test-data true}
```
In case that only side effects need to happen for the data this can be achieved as follows.
```clojure
(qf/quick-file-defrost-iterate "test-coll.data" println)
; {:a 1}
; {:b 2}
; {:c 3}
; => nil
```

### Byte array serialization

The following example demonstrates the byte array serialization.
```clojure
(let [numbers (range 10),
      bytes (qf/quick-byte-freeze numbers)]
  (println "size:" (count bytes))
  (println "array size:" (count (qf/quick-byte-freeze (long-array numbers)))) 
  (qf/quick-byte-defrost bytes))
; size: 24
; array size: 13
; => (0 1 2 3 4 5 6 7 8 9)
```
This above example also demonstrates the benefit of kryo's efficient encoding of small integers:
Instead of using at least 80 bytes for the 10 long numbers, the long array needs only 13 bytes in total.

### Data compression

The use of data compression is configurable. The default compression algorithm is [snappy](https://github.com/xerial/snappy-java) because it is a compromise
with respect to speed and compressed size compared to the built-in GZIP compression.
The following example shows the compression size and compression duration (compress and decompress) tradeoff.
```clojure
(let [xs (vec (range (- Long/MAX_VALUE 100000) Long/MAX_VALUE))
      uncompressed (qf/quick-byte-freeze xs)
      snappy (qf/quick-byte-freeze xs, :compressed true)
      gzip-9 (qf/quick-byte-freeze xs, :compressed true, :compression-algorithm :gzip)
      gzip-1 (qf/quick-byte-freeze xs, :compressed true, :compression-algorithm :gzip, :compression-level 1)]
  (println "uncompressed size:" (count uncompressed))
  (println "snappy size:" (count snappy))
  (println "gzip level 9 size:" (count gzip-9))
  (println "gzip level 1 size:" (count gzip-1)))
; uncompressed size: 1000006
; snappy size: 399531
; gzip level 9 size: 139919
; gzip level 1 size: 140282
; => nil

(use 'criterium.core)

(let [xs (vec (range (- Long/MAX_VALUE 100000) Long/MAX_VALUE))]
  (println "uncompressed:")
  (bench (qf/quick-byte-freeze xs))
  (println "snappy:")
  (bench (qf/quick-byte-freeze xs, :compressed true))
  (println "gzip level 9:")
  (bench (qf/quick-byte-freeze xs, :compressed true, :compression-algorithm :gzip))
  (println "gzip level 1:")
  (bench (qf/quick-byte-freeze xs, :compressed true, :compression-algorithm :gzip, :compression-level 1)))
; uncompressed:
;              Execution time mean :   3.973379 ms
; snappy:
;              Execution time mean :   7.881209 ms
; gzip level 9:
;              Execution time mean : 584.156743 ms
; gzip level 1:
;              Execution time mean :  10.435763 ms

(let [xs (vec (range (- Long/MAX_VALUE 100000) Long/MAX_VALUE))
      uncompressed (qf/quick-byte-freeze xs)
      snappy (qf/quick-byte-freeze xs, :compressed true)
      gzip-9 (qf/quick-byte-freeze xs, :compressed true, :compression-algorithm :gzip)
      gzip-1 (qf/quick-byte-freeze xs, :compressed true, :compression-algorithm :gzip, :compression-level 1)]
  (println "uncompressed:")
  (bench (qf/quick-byte-defrost uncompressed))
  (println "snappy:")
  (bench (qf/quick-byte-defrost snappy, :compressed true))
  (println "gzip level 9:")
  (bench (qf/quick-byte-defrost gzip-9, :compressed true, :compression-algorithm :gzip))
  (println "gzip level 1:")
  (bench (qf/quick-byte-defrost gzip-1, :compressed true, :compression-algorithm :gzip, :compression-level 1)))
; uncompressed:
;              Execution time mean : 3.859958 ms
; snappy:
;              Execution time mean : 4.228060 ms
; gzip level 9:
;              Execution time mean : 6.854080 ms
; gzip level 1
;              Execution time mean : 7.124196 ms
```
The above example is meant as instructional example and not as thorough benchmark of the compression algorithms.

### Incremental creation of files

The namespace ```frost.file-freezer``` offers the types ```Freezer``` and ```Defroster``` which can be used to
incrementally write to or read from a file.
These types are the underlying implementation of the functions in the namespace ```frost.quick-freeze```.


## License

Copyright © 2014-2015 Gunnar Völkel

Distributed under the Eclipse Public License.
