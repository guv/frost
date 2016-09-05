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
    [clojure.core.protocols :as p]
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
  (defrost-coll [this] [this, f, filter-pred] [this, f, filter-pred, max-elements] "Deserialize all remaining objects and return them as a sequential collection.")
  (defrost-coll-chunked [this, chunk-size] [this, chunk-size, f, filter-pred] "Deserialize all remaining objects and return them as a sequential collection. Objects are processed in chunks.")
  (defrost-iterate [this, f, filter-pred] [this, f, filter-pred, max-elements] "Apply a function to all remaining deserialized objects."))



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
(def ^:dynamic *header-buffer-max* (* 1024 1024))

(defn header->bytes
  [header]
  (let [kryo (kryo/default-kryo :registration-required true),
        byte-output (Output. (int *header-buffer-min*), (int *header-buffer-max*))]
    (.writeClassAndObject kryo, byte-output, header)
    (.toBytes byte-output)))


(defn writable-serializer-information
  [header]
  (update-in header [:additional-serializers]
    #(mapv
       (fn [[class, serializer, id? :as spec]]
         (cond-> [(.getName ^Class class), serializer]
           id? (conj id?)))
       %)))


(defn usable-serializer-information
  [header]
  (update-in header [:additional-serializers]
    #(mapv
       (fn [[class-name, serializer, id?]]
         (cond-> [(Class/forName class-name), serializer]
           id? (conj id?)))
       %)))


(defn write-header
  [^OutputStream output-stream, header]
  (let [header (-> header
                 (assoc :frost-version (v/current-version))
                 (cond-> (:additional-serializers header) writable-serializer-information))
        header-bytes (header->bytes header),
        header-length (count header-bytes),        
        data-output (DataOutputStream. output-stream)]
    (.writeInt data-output, header-length)
    (.write data-output, header-bytes, 0, header-length)
    nil))


(defn bytes->header
  [bytes]
  (let [kryo (kryo/default-kryo :registration-required true),
        byte-input (Input. ^bytes bytes)
        header (.readClassAndObject kryo, byte-input)]
    (cond-> header (:additional-serializers header) usable-serializer-information)))


(defn read-header
  [^InputStream input-stream]
  (let [data-input (DataInputStream. input-stream),
        header-length (.readInt data-input),
        header-bytes (make-array Byte/TYPE header-length)]
    (.read data-input header-bytes, 0, header-length)
    (bytes->header header-bytes)))


(defn+opts ^Freezer create-freezer
  "Creates a file freezer for the given filename to serialize data to this file.
  The file freezer can use compression. The file freezer can be created in a thread-safe or non-thread-safe mode.
  <compressed>Specifies whether compression of the data is used.</>
  <file-info>Specifies addition information about the file that is stored in the header of the file.</>
  <locking>Determines whether locking should be use to make the file freezer thread-safe.</>
  "
  [filedesc | {compressed true, file-info nil, locking true} :as options]
  (let [; create file
        file-out (FileOutputStream. (io/file filedesc), false),
        ; header without metadata
        header (with-meta 
                 (select-keys options 
                   [:compressed :no-wrap :compression-algorithm :registration-required :default-serializers :additional-serializers :persistent-metadata :file-info]) 
                 nil)
        ; write header with options
        _ (write-header file-out, header)
        ; activate compression if specified
        file-out (cond-> file-out compressed (compress/wrap-compression options)),
        ; create kryo configured by given parameters
        kryo (kryo/create-specified-kryo options)]
      (Freezer. kryo, (Output. ^OutputStream file-out), filedesc, file-info, (dissoc header :file-info), locking)))



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
  
  (defrost-coll [this, f, filter-pred, max-elements]
    (conditional-wrap locking? (locking this %)
      (loop [element-count 0, result (transient [])]
        (if (< element-count max-elements)
          (if-let [obj (try (.readClassAndObject kryo, in) (catch KryoException e nil))]
            (if (filter-pred obj)
              (recur (unchecked-inc element-count), (conj! result (f obj)))       
              (recur element-count, result))            
            (persistent! result))
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
          (recur)))))
  
  (defrost-iterate [this, f, filter-pred, max-elements]
    (conditional-wrap locking? (locking this %)
      (loop [element-count 0]
        (when (< element-count max-elements)
          (when-let [obj (try (.readClassAndObject kryo, in) (catch KryoException e nil))]
            (if (filter-pred obj)
              (do
                (f obj)
                (recur (unchecked-inc element-count)))
              (recur element-count))))))))


(extend-protocol p/CollReduce

  Defroster

  (coll-reduce
    ([defroster, f]
     (if-let [init (try (defrost defroster) (catch KryoException e nil))]
       (p/coll-reduce defroster, f, init)
       (f)))
    ([defroster, f, init]
     (loop [result init]
       (if-let [value (try (defrost defroster) (catch KryoException e nil))]
         (let [new-result (f result value)]
           (if (reduced? new-result)
             new-result
             (recur new-result)))
         result)))))


(defn+opts ^Defroster create-defroster
  "Creates a file defroster for the given file description to read data from this file.
  The file description can be anything that clojure.java.io/input-stream can handle.
  The file defroster can be created in a thread-safe or non-thread-safe mode.
  <locking>Determines whether locking should be use to make the file freezer thread-safe.</>"
  [filedesc | {locking true}]
  (let [; open file
        file-in (io/input-stream filedesc),        
        ; read header
        {:keys [compressed, file-info, frost-version] :as header} (read-header file-in),
        options (->option-map header),
        ; create kryo configured by given parameters
        kryo (kryo/create-specified-kryo options),
        ; activate compression if specified
        file-in (cond-> file-in
                  (and compressed frost-version) (compress/wrap-compression options)
                  ; before 0.3.0 there was no frost-version and no compression-algorithm written in the file header and only :gzip was supported
                  (and compressed (not frost-version)) (compress/wrap-compression :compression-algorithm :gzip options))]
    (Defroster. kryo, (Input. ^InputStream file-in), filedesc, file-info, (dissoc header :file-info), locking)))


(defn close
  "Closes the given instance of java.io.Closeable."
  [^Closeable x]
  (.close x))