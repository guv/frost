; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns frost.quick-freeze
  (:require
    [clojure.options :refer [defn+opts]]
    (frost
      [byte-freezer :as b]
      [file-freezer :as f]
      [stream-freezer :as s])))



(defn+opts ^"[B" quick-byte-freeze
  "Serializes the given object to a byte array."
  [obj | :as options]
  (let [bf (b/create-byte-freezer options)]
    (b/freeze bf obj)))


(defn+opts quick-byte-defrost
  "Reads an object from the given byte array."
  [bytes | :as options]
  (let [bf (b/create-byte-freezer options)]
    (b/defrost bf bytes)))



(defn+opts quick-stream-freeze
  "Serializes the given object to the given output stream."
  [output-stream, obj | :as options]
  (let [sf (s/create-freezer output-stream, options)]
    (s/freeze sf obj)))


(defn+opts quick-stream-defrost
  "Reads an object from the given input stream."
  [input-stream | :as options]
  (let [sd (s/create-defroster input-stream, options)]
    (s/defrost sd)))



(defn+opts quick-file-freeze
  "Writes the given object to a file with the given filename overwriting the file in case it already exists."
  [filename, obj | :as options]
  (with-open [ff (f/create-freezer filename, options)]
    (f/freeze ff, obj)))


(defn+opts quick-file-freeze-coll
  "Writes the given collection of objects to a file with the given filename overwriting the file in case it already exists.
  The objects are written to the file consecutively - the collection itself is not stored."
  [filename, coll | :as options]
  (with-open [ff (f/create-freezer filename, options)]
    (f/freeze-coll ff, coll)))


(defn+opts quick-file-defrost
  "Reads the first object from the file with the given file description.
  The file description can be anything that clojure.java.io/input-stream can handle.
  <process-fn>Instead of the read object the result of the function application to the object is returned.</>"
  [filedesc | {process-fn identity} :as options]
  (with-open [fd (f/create-defroster filedesc, options)]
    (f/defrost fd, process-fn)))


(defn+opts quick-file-defrost-coll
  "Reads all objects from the file with the given file description. Returns a collection of those objects.
  The file description can be anything that clojure.java.io/input-stream can handle.
  <process-fn>Instead of the read objects the result of the function applications to the objects is returned.</>
  <filter-pred>Function that decides whether an object is returned (including process-fn application) or not.</>
  <max-elements>Limit the number of elements in the result collection</>"
  [filedesc | {process-fn identity, filter-pred (constantly true), max-elements nil} :as options]
  ; :filter-pred nil is equivalent to :filter-pred (constantly true)
  (let [filter-pred (or filter-pred (constantly true))]
    (with-open [fd (f/create-defroster filedesc, options)]
      (if max-elements
        (f/defrost-coll fd, process-fn, filter-pred, max-elements)
        (f/defrost-coll fd, process-fn, filter-pred)))))


(defn+opts quick-file-defrost-coll-chunked
  "Reads all objects from the file with the given file description. Returns a collection of those objects.
  The specified function is applied to chunks of objects instead of single objects.
  The file description can be anything that clojure.java.io/input-stream can handle.
  <process-fn>Instead of the read objects the result of the function applications to the objects is returned.</>
  <filter-pred>Function that decides whether an object is returned (including process-fn application) or not.</>"
  [filedesc, chunk-size | {process-fn identity, filter-pred (constantly true)} :as options]
  (with-open [fd (f/create-defroster filedesc, options)]
    (f/defrost-coll-chunked fd, chunk-size, process-fn, filter-pred)))


(defn+opts quick-file-defrost-iterate
  "Applies the given function to all deserialized objects in the file with the given file description.
  The file description can be anything that clojure.java.io/input-stream can handle.
  <filter-pred>Function that decides whether the given function is applied to an object.</>"
  [filedesc, process-fn | {filter-pred (constantly true), max-elements nil} :as options]
  (with-open [fd (f/create-defroster filedesc, options)]
    (if max-elements
      (f/defrost-iterate fd, process-fn, filter-pred, max-elements)
      (f/defrost-iterate fd, process-fn, filter-pred))))


(defn+opts quick-file-header
  "Reads the file header of the file with the given file description.
  The file description can be anything that clojure.java.io/input-stream can handle."
  [filedesc | :as options]
  (with-open [ff (f/create-defroster filedesc, options)]
    (f/file-header ff)))


(defn+opts quick-file-info
  "Reads the user-provided file info of the file with the given file description.
  The file description can be anything that clojure.java.io/input-stream can handle."
  [filedesc | :as options]
  (with-open [ff (f/create-defroster filedesc, options)]
    (f/file-info ff)))