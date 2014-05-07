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
    [frost.analysis :as analysis])
  (:import 
    (com.esotericsoftware.kryo Kryo Serializer)))


(defn+opts ^Kryo register-serializers
  "Registers the given serializers at the given kryo instance.
  The format for the serializers list is supposed to look like:
  [[Class1 serializer1 id1?] [Class2 serializer2 id2?] ...]
  <analyze>Determines if the serializers supports analysis via frost.analysis.</>"
  [^Kryo kryo, serializers | {analyze false}]
  (doseq [[^Class clazz, ^Serializer serializer, id :as all] serializers]
    (if serializer
      (let [serializer (if analyze (analysis/analysis-serializer serializer) serializer)] 
        (if id
          ; when serializer and id are given
	        (.register kryo clazz serializer (int id))
	        ; when only a serializer is given
	        (.register kryo clazz serializer)))
      ; when only a class is given
      (.register kryo clazz)))
  kryo)



(defn+opts ^Kryo default-serializers
  "Register the defaul serializers for Java and Clojure data at the given kryo instance."
  [^Kryo kryo | :as options]
  (doto kryo
    (register-serializers serializers/java-serializers, options)
    (register-serializers (serializers/clojure-serializers options), options)))


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
  (let [reg (default-serializers (create-kryo registration-required), options)]
    (if additional-serializers
      (register-serializers reg, additional-serializers, options)
      reg)))