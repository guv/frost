; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns frost.kryo
  (:require
    [clojure.options :refer [defn+opts]]
    [frost.serializers :as serializers]
    [frost.analysis :as analysis]
    [frost.util :as u])
  (:import 
    (com.esotericsoftware.kryo Kryo Serializer)))



(defn+opts ^Kryo register-serializers
  "Registers the given serializers at the given kryo instance.
  The format for the serializers list is supposed to look like:
  [[Class1 serializer1 id1?] [Class2 serializer2 id2?] ...]
  <analyze>Determines if the serializers supports analysis via frost.analysis.</>"
  [^Kryo kryo, serializers | {persistent-meta-data true, analyze false}]
  (doseq [[^Class clazz, ^Serializer serializer, id :as all] serializers]
    (if serializer
      (let [serializer (if (symbol? serializer)
                         (if-let [serializer-constructor (u/resolve-fn serializer)]
                           (serializer-constructor persistent-meta-data)
                           (u/illegal-argument "Function %s could not be resolved!" serializer))
                         serializer)
            serializer (if analyze (analysis/analysis-serializer serializer) serializer)] 
        (if id
          ; when serializer and id are given
	        (.register kryo clazz serializer (int id))
	        ; when only a serializer is given
	        (.register kryo clazz serializer)))
      ; when only a class is given
      (.register kryo clazz)))
  kryo)



(defn+opts ^Kryo register-default-serializers
  "Register the defaul serializers for Java and Clojure data at the given kryo instance.
  <persistent-metadata>Specifies if the metadata is persisted as well.</persistent-meta>"
  [^Kryo kryo | {persistent-meta-data true} :as options]
  (doto kryo
    (register-serializers serializers/java-serializers, options)
    (register-serializers (serializers/clojure-serializers persistent-meta-data), options)))


(defn ^Kryo create-kryo
  "Creates a kryo instance specifying whether the classes must be registered with their serializers in advance."
  [registration-required]
  (let [kryo (Kryo.)]
    (.setRegistrationRequired ^Kryo kryo registration-required)
    kryo))


(defn+opts ^Kryo default-kryo
  "Creates a kryo instance with the default serializers for Java and Clojure data.
  <registration-required>Determines whether each class of an object to be serialized needs to be registered.</>
  <additional-serializers>Can be used to specify a list of additional serializers. Format per class is [class serializer id?].</>"
  [| {registration-required true, additional-serializers nil} :as options]
  (let [reg (register-default-serializers (create-kryo registration-required), options)]
    (if additional-serializers
      (register-serializers reg, additional-serializers, options)
      reg)))


(defn+opts create-specified-kryo
  "Create a kryo instance via the specified options.
  <registration-required>Determines whether each class of an object to be serialized needs to be registered.</>
  <default-serializers>Specifies whether the default serializers for Clojure and Java data are used. You usually want these.</>
  <additional-serializers>Can be used to specify a list of additional serializers. Format per class is [class serializer id?].</>"
  [| {additional-serializers nil, registration-required true, default-serializers true} :as options]
  (cond-> (create-kryo registration-required)
    ; register default serializers if specified
    default-serializers (register-default-serializers options)
    ; register additional serializers if specified
    additional-serializers (register-serializers additional-serializers, options)))