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
    [frost.util :as u])
  (:import
    (com.esotericsoftware.kryo Kryo Serializer)))



(defn try-resolve-analysis-serializer
  []
  (-> 'frost.analysis/analysis-serializer resolve var-get))


(defn maybe-add-analysis
  "Wraps the given serializer in an anylsis serializer provided that the library frost.analysis is available."
  ^Serializer [^Serializer serializer]
  (if-let [wrap-analysis (try
                           (if-let [wrap-analysis (try-resolve-analysis-serializer)]
                             wrap-analysis
                             (do
                               (require 'frost.analysis)
                               (try-resolve-analysis-serializer)))
                           (catch Throwable _
                             nil))]
    (wrap-analysis serializer)
    (do
      (u/printf-err "Given :analyze true but namespace frost.analysis could not be loaded or function frost.analysis/analysis-serializer could not be resolved.\nHave you added frost-analysis as dependency?")
      serializer)))


(defn resolve-serializer
  [serializer-symbol, resolve-error-handler]
  (let [f (try
            (u/resolve-fn serializer-symbol)
            (catch Throwable t
              t))]
    (if (or (nil? f) (instance? Exception f))
      (if-let [g (when resolve-error-handler (resolve-error-handler serializer-symbol))]
        g
        (if (nil? f)
          (u/illegal-argument "Function %s could not be resolved!" serializer-symbol)
          (throw
            (RuntimeException.
              (format "Serializer \"%s\" could not be resolved!" serializer-symbol)
              f))))
      f)))


(defn+opts ^Kryo register-serializers
  "Registers the given serializers at the given kryo instance.
  The format for the serializers list is supposed to look like:
  [[Class1 serializer1 id1?] [Class2 serializer2 id2?] ...]
  <analyze>Determines if the serializers supports analysis via frost.analysis.</>
  <resolve-error-handler>If a serializer constructor cannot be resolved, this function will be called to provide an alternative serializer constructor.</>"
  [^Kryo kryo, serializers | {persistent-meta-data true, analyze false, resolve-error-handler nil}]
  (doseq [[^Class clazz, ^Serializer serializer, id :as all] serializers]
    (if serializer
      (let [^Serializer serializer (cond
                                     (symbol? serializer)
                                     (let [serializer-constructor (resolve-serializer serializer, resolve-error-handler)]
                                       (serializer-constructor persistent-meta-data))

                                     (instance? Serializer serializer)
                                     serializer

                                     :else
                                     (u/illegal-argument
                                       "Serializer for class \"%s\" must be either a Serializer instance or a symbol refering to a constructor function!"
                                       (.getCanonicalName clazz)))
            serializer (if analyze (maybe-add-analysis serializer) serializer)]
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