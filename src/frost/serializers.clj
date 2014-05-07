; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns frost.serializers
  (:require
    [clojure.string :as string]
    [clojure.options :refer [defn+opts]])
  (:import
    (com.esotericsoftware.kryo Kryo Serializer)
    (com.esotericsoftware.kryo.io Input Output)
    (clojure.lang
      PersistentVector PersistentHashSet PersistentTreeSet MapEntry
      Cons PersistentList$EmptyList PersistentList LazySeq IteratorSeq
      PersistentArrayMap PersistentHashMap PersistentTreeMap PersistentStructMap
      ArraySeq
      Keyword Symbol StringSeq Ratio)
    (java.util Date UUID)
    java.util.regex.Pattern
    (java.math BigDecimal BigInteger)
    (com.esotericsoftware.kryo.serializers
      DefaultArraySerializers$ByteArraySerializer,
      DefaultArraySerializers$CharArraySerializer,
      DefaultArraySerializers$DoubleArraySerializer,
      DefaultArraySerializers$FloatArraySerializer,
      DefaultArraySerializers$IntArraySerializer,
      DefaultArraySerializers$LongArraySerializer,
      DefaultArraySerializers$ObjectArraySerializer,
      DefaultArraySerializers$ShortArraySerializer,
      DefaultArraySerializers$StringArraySerializer,
      DefaultSerializers$DateSerializer
      DefaultSerializers$BigIntegerSerializer
      DefaultSerializers$BigDecimalSerializer)
    (java.nio
      ByteBuffer
      DoubleBuffer)))



(defmacro serializer
  [& body]
 `(proxy [Serializer] [] ~@body))


(defmacro loop-times
  [times, [result-name init-binding], body]
 `(loop [i# ~times, ~result-name ~init-binding]
    (if (pos? i#)
      (recur (dec i#), ~body)
      ~result-name)))



(declare ^Serializer metadata-serializer)

(defn write-metadata
  [^Kryo kryo, ^Output out, obj]
  (let [metadata (meta obj)]
    (if metadata
      (do
        (.writeClass kryo out, (class metadata))
        (.writeObjectOrNull kryo out, metadata, metadata-serializer))
      (.writeClass kryo out, nil))))

(defn read-metadata
  [^Kryo kryo, ^Input in]
  (let [cls (.readClass kryo in)]
    (when cls
      (.readObjectOrNull kryo in, (.getType cls), metadata-serializer))))

(defmacro maybe-write-metadata
  [meta?, [kryo, out, obj], & write-body]
 `(do ~@write-body
    (when ~meta?
      (write-metadata ~kryo, ~out, ~obj))))

(defmacro maybe-read-metadata
  [meta?, [kryo, in], read-body]
 `(let [obj# ~read-body]
    (if ~meta?
      (let [metadata# (read-metadata ~kryo, ~in)]
        (if metadata#
          (with-meta obj# metadata#)
          obj#))
      obj#)))



(defn write-map
  [^Kryo kryo, ^Output out, m]
  (.writeInt out (count m), true)
  (doseq [[k v] m]
    (.writeClassAndObject kryo, out, k)
    (.writeClassAndObject kryo, out, v)))


(defn read-map
  [^Kryo kryo, ^Input in]
  (persistent!
    (loop-times (.readInt in, true) [m (transient {})]
      (let [k (.readClassAndObject kryo, in)
            v (.readClassAndObject kryo, in)]
        (assoc! m k v)))))


(defn ^Serializer map-serializer
  [meta?]
  (serializer
    (write [kryo, out, m]
      (maybe-write-metadata meta? [kryo, out, m]
        (write-map kryo, out, m)))
    (read [kryo, in, clazz]
      (maybe-read-metadata meta? [kryo, in]
        (read-map kryo, in)))))



(def ^Serializer metadata-serializer (map-serializer false))



(defn write-sequential
  "Writes a sequential data structure."
  [^Kryo kryo, ^Output out, coll]
  (.writeInt out (count coll), true)
  (doseq [obj coll]
    (.writeClassAndObject kryo, out, obj)))

(defn read-sequential
  "Reads a sequential data structure into a vector."
  [^Kryo kryo, ^Input in, init-coll]
  (persistent!
    (loop-times (.readInt in, true) [coll (transient init-coll)]
      (conj! coll (.readClassAndObject kryo, in)))))


(defn ^Serializer collection-serializer
  [init-coll, meta?]
  (serializer
    (write [kryo, out, coll]
      (maybe-write-metadata meta? [kryo, out, coll]
        (write-sequential kryo, out, coll)))
    (read [kryo, in, clazz]
      (maybe-read-metadata meta? [kryo, in]
        (read-sequential kryo, in, init-coll)))))


(defn ^Serializer list-serializer
  [meta?]
  (serializer
    (write [kryo, out, lst]
      (maybe-write-metadata meta? [kryo, out, lst]
        (write-sequential kryo, out, lst)))
    (read [kryo, in, clazz]
      (maybe-read-metadata meta? [kryo, in]
        (apply list (read-sequential kryo, in, []))))))



(def ^Serializer reader-serializer
  (serializer
    (write [kryo, ^Output out, k]
      (.writeString out (pr-str k)))
    (read [kryo, ^Input in, clazz]
      (read-string (.readString in)))))


(defn ^Serializer symbol-serializer
  [meta?]
  (serializer
    (write [kryo, ^Output out, sym]
      (maybe-write-metadata meta? [kryo, out, sym]
        (.writeString out (pr-str sym))))
    (read [kryo, ^Input in, clazz]
      (maybe-read-metadata meta? [kryo, in]
        (read-string (.readString in))))))


(defn ^Serializer stringseq-serializer
  [meta?]
  (serializer
    (write [kryo, ^Output out, strseq]
      (maybe-write-metadata meta? [kryo, out, strseq]
        (.writeString out (string/join strseq))))
    (read [kryo, ^Input in, clazz]
      (maybe-read-metadata meta? [kryo, in]
        (seq (.readString in))))))


(def ^Serializer ratio-serializer
  (let [bigint-serializer (new DefaultSerializers$BigIntegerSerializer)]
    (serializer
	    (write [^Kryo kryo, ^Output out, ratio]
        (.write bigint-serializer kryo, out, (numerator ratio))
	      (.write bigint-serializer kryo, out, (denominator ratio)))
	    (read [^Kryo kryo, ^Input in, clazz]
	      (let [n (.read bigint-serializer kryo, in, nil)
              d (.read bigint-serializer kryo, in, nil)]
          (Ratio. n, d))))))


(def ^Serializer uuid-serializer
  (serializer
    (write [^Kryo kryo, ^Output out, ^UUID uuid]
      (.writeLong out, (.getMostSignificantBits  uuid), false)
      (.writeLong out, (.getLeastSignificantBits uuid), false))
    (read [^Kryo kryo, ^Input in, clazz]
      (let [most  (.readLong in false),
            least (.readLong in false)]
        (UUID. most, least)))))



(defmacro apply-serializer-to
  [serializer, & classes]
  (let [s (gensym "serializer")] 
   `(let [~s ~serializer] 
      (vector
        ~@(for [c classes :when c] 
            ; if a vector is given ...
            (cond 
              (and (symbol? c) (class? (resolve c)))  
                [c s]
              (vector? c) 
                (let [[cls id] c]
                  [cls s id])
              :else 
                (throw (IllegalArgumentException. "Only a class or a vector [class, id] are allowed!"))))))))


(defn+opts clojure-serializers
  "Returns a vector of serializer registrations (class serializer pairs) for the standard datastructures of Clojure.
  <persistent-metadata>Specifies if the metadata is persisted as well.</persistent-meta>"
  [| {persistent-metadata true}]
  (concat
    ; vectors
    (apply-serializer-to (collection-serializer  [], persistent-metadata)
      [PersistentVector 40],
      [MapEntry         41]
      [ArraySeq         58])
    ; sets 
    (apply-serializer-to (collection-serializer #{}, persistent-metadata)
      [PersistentHashSet 42],
      [PersistentTreeSet 43])
    ; lists
    (apply-serializer-to (list-serializer persistent-metadata)
      [PersistentList 44],
      [PersistentList$EmptyList 45],
      [LazySeq        46],
      [Cons           47],
      [IteratorSeq    48])
    ; maps
    (apply-serializer-to (map-serializer persistent-metadata)
      [PersistentArrayMap  49],
      [PersistentHashMap   50],
      [PersistentTreeMap   51],
      [PersistentStructMap 52])
    ; keyword, pattern, symbol
    [[Keyword reader-serializer 53]
     [Pattern reader-serializer 54]
     [Symbol    (symbol-serializer persistent-metadata)    55]
     [StringSeq (stringseq-serializer persistent-metadata) 56]
     [Ratio     ratio-serializer                           57]]
    
    ; BigInt if it exists
    (when-let [cls (try (Class/forName "clojure.lang.BigInt") (catch ClassNotFoundException _))]
        [[cls reader-serializer 100]])))


(def java-serializers
  (concat
    [[Date       (new DefaultSerializers$DateSerializer)       20]
     [BigInteger (new DefaultSerializers$BigIntegerSerializer) 21]
     [BigDecimal (new DefaultSerializers$BigDecimalSerializer) 22]
     [UUID       uuid-serializer                               23]]
    (map
	    #(vector (Class/forName (str "[" %1)) (.newInstance ^Class %2) %3)
	    ["B" "C" "D" "F" "I" "J" "Ljava.lang.Object;" "S" "Ljava.lang.String;"]
	    [DefaultArraySerializers$ByteArraySerializer,
	     DefaultArraySerializers$CharArraySerializer,
	     DefaultArraySerializers$DoubleArraySerializer,
	     DefaultArraySerializers$FloatArraySerializer,
	     DefaultArraySerializers$IntArraySerializer,
	     DefaultArraySerializers$LongArraySerializer,
	     DefaultArraySerializers$ObjectArraySerializer,
	     DefaultArraySerializers$ShortArraySerializer,
	     DefaultArraySerializers$StringArraySerializer]
	     (range 24 40))))