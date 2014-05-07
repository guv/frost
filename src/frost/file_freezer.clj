; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns frost.file-freezer
  (:require
    [clojure.java.io :as io]
    [clojure.options :refer [defn+opts, defn+opts-, ->option-map]]
    [frost.util :refer [conditional-wrap]]
    [frost.version :as v]
    [frost.kryo :as kryo]
    [frost.compression :as compress])
  (:import
    (java.io Closeable FileOutputStream FileInputStream EOFException OutputStream InputStream DataOutputStream DataInputStream)
    (com.esotericsoftware.kryo Kryo KryoException)
    (com.esotericsoftware.kryo.io Output Input)))



(defprotocol IInfo 
  (resource-id [this] "Returns the name of the resource (usually file name).")
  (file-info [this] "Returns the user-provided file info.")
  (file-header [this] "Returns the file header."))

(defprotocol IFreeze
  (freeze [this, obj] "Serialize a given object.")
  (freeze-coll [this, coll] "Serialize a given collection of objects."))

(defprotocol IDefrost
  (defrost [this] [this, f] "Deserialize the next object.")
  (defrost-coll [this] [this, f, filter-pred] "Deserialize all remaining objects and return them as a sequential collection.")
  (defrost-coll-chunked [this, chunk-size] [this, chunk-size, f, filter-pred] "Deserialize all remaining objects and return them as a sequential collection. Objects are processed in chunks.")
  (defrost-iterate [this, f, filter-pred] "Apply a function to all remaining deserialized objects."))



(deftype Freezer [^Kryo kryo, ^Output out, filename, info, header, locking?]
  IInfo
  (resource-id [this] filename)
  (file-info [this] info)
  (file-header [this] header)
  
  Closeable
  (close [this]
    (conditional-wrap locking? (locking this %)
      (.flush out)
      (.close out)))
  
  IFreeze
  (freeze [this, obj]
    (conditional-wrap locking? (locking this %)
      (.writeClassAndObject kryo, out, obj)
      (.flush out)))
  (freeze-coll [this, coll]
    (conditional-wrap locking? (locking this %)
      (loop [obj-seq (seq coll)]
		    (when obj-seq
		      (.writeClassAndObject kryo, out, (first obj-seq))
		      (recur (next obj-seq))))
		  (.flush out))))


(def ^:dynamic *header-buffer-min* 1024)
(def ^:dynamic *header-buffer-max* 102400)

(defn header->bytes
  [header]
  (let [kryo (kryo/default-kryo :registration-required true),
        byte-output (Output. (int *header-buffer-min*), (int *header-buffer-max*))]
    (.writeClassAndObject kryo, byte-output, header)
    (.toBytes byte-output)))

(defn write-header
  [^OutputStream output-stream, header]
  (let [header-bytes (header->bytes (assoc header :frost-version (v/current-version))),
        header-length (count header-bytes),        
        data-output (DataOutputStream. output-stream)]
    (.writeInt data-output, header-length)
    (.write data-output, header-bytes, 0, header-length)
    nil))


(defn bytes->header
  [bytes]
  (let [kryo (kryo/default-kryo :registration-required true),
        byte-input (Input. ^bytes bytes)]
    (.readClassAndObject kryo, byte-input)))

(defn read-header
  [^InputStream input-stream]
  (let [data-input (DataInputStream. input-stream),
        header-length (.readInt data-input),
        header-bytes (make-array Byte/TYPE header-length)]
    (.read data-input header-bytes, 0, header-length)
    (bytes->header header-bytes)))



(defn+opts create-specified-kryo
  [additional-serializers | :as options]
  (let [{:keys [registration-required, default-serializers, persistent-metadata]} options,
        ; create kryo
        kryo (kryo/create-kryo registration-required),
        ; register default serializers if specified
        kryo (if default-serializers (kryo/default-serializers kryo :persistent-metadata persistent-metadata, options) kryo),
        ; register additional serializers if specified
        kryo (if additional-serializers (kryo/register-serializers kryo, additional-serializers, options) kryo)]
    kryo))


(defn+opts ^Freezer create-freezer
  "Creates a file freezer for the given filename to serialize data to this file.
  The file freezer can use compression. The file freezer can be created in a thread-safe or non-thread-safe mode.
  <compressed>Specifies whether compression of the data is used.</>
  <no-wrap>For compression algorithm :gzip, set to true for GZIP compatible compression</>
  <compression-level>For compression algorithm :gzip, range 0,1-9 (no compression, best speed - best compression)</>
  <compression-algorithm>Specifies the compression algorithm to use. Snappy is default since it is pretty fast when reading and writing.</>
  <registration-required>Determines whether each class of an object to be serialized needs to be registered.</>
  <default-serializers>Specifies whether the default serializers for Clojure and Java data are used. You usually want these.</>
  <persistent-metadata>Specifies whether metadata of Clojure data is stored as well.</>
  <additional-serializers>Can be used to specify a list of additional serializers. Format per class is [class serializer id?].</>
  <file-info>Specifies addition information about the file that is stored in the header of the file.</>
  <locking>Determines whether locking should be use to make the file freezer thread-safe.</>
  "
  [filename | {compressed true, no-wrap true, compression-level (choice 9 0 1 2 3 4 5 6 7 8), compression-algorithm (choice :snappy :gzip),
               registration-required true, default-serializers true, persistent-metadata true, additional-serializers nil,
               file-info nil, locking true} :as options]
  (let [; create file
        file-out (FileOutputStream. ^String filename, false),
        ; header without metadata
        header (with-meta 
                 (select-keys options 
                   [:compressed :no-wrap :compression-algorithm :registration-required :default-serializers :persistent-metadata :file-info]) 
                 nil)
        ; write header with options
        _ (write-header file-out, header)
        ; activate compression if specified
        file-out (cond-> file-out compressed (compress/wrap-compression options)),
        ; create kryo configured by given parameters
        kryo (create-specified-kryo additional-serializers, options)]
      (Freezer. kryo, (Output. ^OutputStream file-out), filename, file-info, (dissoc header :file-info), locking)))



(deftype Defroster [^Kryo kryo, ^Input in, filedesc, info, header, locking?]
  IInfo
  (resource-id [this] filedesc)
  (file-info [this] info)
  (file-header [this] header)
  
  Closeable
  (close [this]
    (conditional-wrap locking? (locking this %)
      (.close in)))
  
  IDefrost
  (defrost [this]
    (defrost this, identity))
  
  (defrost [this, f]
    (conditional-wrap locking? (locking this %)
      (f (.readClassAndObject kryo, in))))
    
  (defrost-coll [this]
    (defrost-coll this, identity, (constantly true)))
  
  (defrost-coll [this, f, filter-pred]
    (conditional-wrap locking? (locking this %)
      (loop [result (transient [])]
        (if-let [obj (try (.readClassAndObject kryo, in) (catch KryoException e nil))]
          (recur (if (filter-pred obj)
                   (conj! result (f obj))
                   result))
          (persistent! result)))))
  
  (defrost-coll-chunked [this, chunk-size]
    (defrost-coll-chunked this, chunk-size, identity, (constantly true)))
  
  (defrost-coll-chunked [this, chunk-size, f, filter-pred]
    (conditional-wrap locking? (locking this %)
      (let [chunk-size (long chunk-size)]
        (loop [chunk (transient []), result (transient [])]
          (if-let [obj (try (.readClassAndObject kryo, in) (catch KryoException e nil))]
            (let [chunk (cond-> chunk (filter-pred obj) (conj! obj))]
              ; if chunk size reached, ...  
              (if (= (count chunk) chunk-size)
                ; ... then start new chunk, apply function to chunk and add return values to result ... 
                (recur (transient []), (conj! result (f (persistent! chunk))))
                ; ... else continue with reading.
                (recur chunk, result)))
            (persistent!
              (cond-> result
                (pos? (count chunk))
                (conj! (f (persistent! chunk))))))))))
  
  (defrost-iterate [this, f, filter-pred]
    (conditional-wrap locking? (locking this %)
      (loop []
        (when-let [obj (try (.readClassAndObject kryo, in) (catch KryoException e nil))]
          (when (filter-pred obj)
            (f obj))
          (recur))))))


(defn+opts ^Defroster create-defroster
  "Creates a file defroster for the given file description to read data from this file.
  The file description can be anything that clojure.java.io/input-stream can handle.
  The file defroster can be created in a thread-safe or non-thread-safe mode.
  <additional-serializers>Needs to be used to specify a list of additional serializers that were used when the file was written. 
  Format per class is [class serializer id?].</>
  <locking>Determines whether locking should be use to make the file freezer thread-safe.</>"
  [filedesc | {additional-serializers nil, locking true}]
  (let [; open file
        file-in (io/input-stream filedesc),        
        ; read header
        {:keys [compressed, file-info, frost-version] :as header} (read-header file-in),
        options (->option-map header),
        ; create kryo configured by given parameters
        kryo (create-specified-kryo additional-serializers, options),
        ; activate compression if specified
        file-in (cond-> file-in
                  (and compressed frost-version) (compress/wrap-compression options)
                  ; before 0.3.0 there was no frost-version and no compression-algorithm written in the file header and only :gzip was supported
                  (and compressed (not frost-version)) (compress/wrap-compression :compression-algorithm :gzip options))]
    (Defroster. kryo, (Input. ^InputStream file-in), filedesc, file-info, (dissoc header :file-info), locking)))