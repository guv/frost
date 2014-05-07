; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns frost.byte-freezer
  (:require
    [clojure.options :refer [defn+opts, ->option-map]]
    [frost.util :refer [conditional-wrap]]
    [frost.kryo :as kryo]
    [frost.compression :as compress])
  (:import
    (com.esotericsoftware.kryo Kryo)
    (com.esotericsoftware.kryo.io Output Input)
    (java.io ByteArrayOutputStream ByteArrayInputStream InputStream OutputStream)))


(defprotocol IByteFreezer
  (freeze [this, obj] "Freezes the given object to bytes.")
  (defrost [this, obj-bytes] "Defrosts the object from the given bytes"))


(deftype ByteFreezer [^Kryo kryo, ^Output output, ^ByteArrayOutputStream byte-out]
  IByteFreezer
  (freeze [this, obj]
    (.reset byte-out)
    (try 
      (.setOutputStream output byte-out)
      (.writeClassAndObject kryo, output, obj)
      (.flush output)
      (.toByteArray byte-out)
      (finally
        (.clear output)
        (.setOutputStream output nil))))
  (defrost [this, obj-bytes]
    (with-open [byte-in (ByteArrayInputStream. obj-bytes),          
          in (Input. byte-in)]      
      (.readClassAndObject kryo, in))))


(deftype CompressedByteFreezer [^Kryo kryo, ^Output output, ^ByteArrayOutputStream byte-out, options]
  IByteFreezer
  (freeze [this, obj]
    (.reset byte-out)
    (let [^OutputStream zip-out (compress/wrap-compression byte-out, (->option-map options))]
      (try 
        (.setOutputStream output zip-out)
        (.writeClassAndObject kryo, output, obj)
        (.flush output)
        (.close zip-out)
        (.toByteArray byte-out)
        (finally
          (.clear output)
          (.setOutputStream output nil)))))
  (defrost [this, obj-bytes]
    (let [byte-in (ByteArrayInputStream. obj-bytes),
          ^InputStream zip-in (compress/wrap-compression byte-in, (->option-map options)),
          in (Input. zip-in)]
      (try
        (.readClassAndObject kryo, in)
        (finally
          (.close in)
          (.close zip-in))))))


; not thread safe (no locking)
(defn+opts create-byte-freezer
  "Creates a byte freezer that can be used to write objects to byte arrays and read them back again.
  The byte freezer can use compression. The size of its byte buffer can be constrained.
  <compressed>Specifies whether compression of the data is used.</>
  <no-wrap>For compression algorithm :gzip, set to true for GZIP compatible compression</>
  <compression-level>For compression algorithm :gzip, range 0,1-9 (no compression, best speed - best compression)</>
  <compression-algorithm>Specifies the compression algorithm to use. Snappy is default since it is pretty fast when reading and writing.</>
  <initial-buffer>Initial size of the buffer that is used for writing data.</>
  <max-buffer>Maximal size of the buffer that is used for writing data.</>"
  [| {compressed false, no-wrap true, compression-level (choice 9 0 1 2 3 4 5 6 7 8), compression-algorithm (choice :snappy :gzip),
      initial-buffer (* 1024 1024), max-buffer (* 128 1024 1024)} :as options]
  (let [registry (kryo/default-kryo options),
        output (Output. (int initial-buffer), (int max-buffer)),
        byte-out (ByteArrayOutputStream. (int initial-buffer))]
    (if compressed
      (CompressedByteFreezer. registry, output, byte-out, options)
      (ByteFreezer. registry, output, byte-out))))