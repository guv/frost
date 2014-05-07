; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns test.frost.quick-freeze
  (:require
    [frost.quick-freeze :as qf]
    [clojure.options :refer [defn+opts]]
    [midje.sweet :refer :all]
    [clojure.test.check :as c]
    (clojure.test.check
      [generators :as gen]
      [properties :as prop]))
  (:import
    java.util.UUID
    java.util.regex.Pattern))


(def ^:const repetitions 200)



(defn array-roundtrip
  [type, element-gen]
  (prop/for-all [v (gen/vector element-gen)]
    (let [a (into-array type, v),
          b (-> a qf/quick-byte-freeze qf/quick-byte-defrost)]
      (and
        (=   (class a)   (class b))
        (= (alength a) (alength b))
        (every? #(= (aget a %) (aget b %)) (range (alength a)))))))


(def short-gen
  (gen/fmap short (gen/choose Short/MIN_VALUE Short/MAX_VALUE)))

(def int-gen
  (gen/fmap int (gen/choose Integer/MIN_VALUE Integer/MAX_VALUE)))

(def long-gen
  (gen/fmap long (gen/choose Long/MIN_VALUE Long/MAX_VALUE)))


(fact "byte array roundtrip"
  (c/quick-check repetitions (array-roundtrip Byte/TYPE gen/byte)) => (contains {:result true}))

(fact "short array roundtrip"
  (c/quick-check repetitions (array-roundtrip Short/TYPE short-gen)) => (contains {:result true}))

(fact "int array roundtrip"
  (c/quick-check repetitions (array-roundtrip Integer/TYPE int-gen)) => (contains {:result true}))

(fact "long array roundtrip"
  (c/quick-check repetitions (array-roundtrip Long/TYPE long-gen)) => (contains {:result true}))




(def uuid-gen
  (gen/fmap #(UUID. (first %) (second %)) (gen/vector long-gen 2)))

(def uuid-roundtrip
  (prop/for-all [id uuid-gen]
    (-> id qf/quick-byte-freeze qf/quick-byte-defrost (= id))))

(fact "UUID roundtrip"
  (c/quick-check repetitions uuid-roundtrip) => (contains {:result true}))




(def set-any-gen
  (gen/fmap set (gen/vector gen/simple-type)))

(def set-roundtrip
  (prop/for-all [s set-any-gen]
    (-> s qf/quick-byte-freeze qf/quick-byte-defrost (= s))))

(fact "set roundtrip"
  (c/quick-check repetitions set-roundtrip) => (contains {:result true}))



; TODO: write generators to cover all supported types (arrays, ratios, sets, symbols, uuids) cf. frost.serializers

; nested types of gen/any are vectors, lists and maps (why not sets?)
(defn+opts roundtrip-anything-property
  [| :as options]
  (prop/for-all [x gen/any]
    (-> x (qf/quick-byte-freeze options) (qf/quick-byte-defrost options) (= x))))


(fact "roundtrip anything generated by gen/any"
  (c/quick-check repetitions (roundtrip-anything-property) :max-size 100) => (contains {:result true}))


(fact "roundtrip anything generated by gen/any with snappy compression"
  (c/quick-check repetitions (roundtrip-anything-property :compressed true, :compression-algorithm :snappy) :max-size 100) => (contains {:result true}))


(fact "roundtrip anything generated by gen/any with GZIP compression"
  (c/quick-check repetitions (roundtrip-anything-property :compressed true, :compression-algorithm :gzip) :max-size 100) => (contains {:result true}))