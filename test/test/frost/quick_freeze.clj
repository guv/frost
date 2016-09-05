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
    [clojure.test.check :as c]
    (clojure.test.check
      [generators :as gen]
      [properties :as prop])
    [clojure.test.check.clojure-test :as test])
  (:import
    java.util.UUID))


(def ^:const repetitions 200)


(defn same-array-values?
  [get-fn, length-fn, a, b]
  (let [n (length-fn a)]
    (loop [i 0]
      (if (< i n)
        (if (= (get-fn a i) (get-fn b i))
          (recur (unchecked-inc i))
          false)
        true))))


(defn array-roundtrip
  [type, get-fn, length-fn, element-gen]
  (prop/for-all [v (gen/vector element-gen)]
    (let [a (into-array type, v),
          b (-> a qf/quick-byte-freeze qf/quick-byte-defrost)]
      (and
        (=   (class a)   (class b))
        (= (length-fn a) (length-fn b))
        (same-array-values? get-fn, length-fn, a, b)))))


(def short-gen
  (gen/fmap short (gen/choose Short/MIN_VALUE Short/MAX_VALUE)))

(def int-gen
  (gen/fmap int (gen/choose Integer/MIN_VALUE Integer/MAX_VALUE)))

(def long-gen
  (gen/fmap long (gen/choose Long/MIN_VALUE Long/MAX_VALUE)))

(def float-gen
  (gen/fmap float gen/ratio))

(def double-gen
  (gen/fmap double gen/ratio))


(test/defspec byte-array-roundtrip repetitions
  (array-roundtrip Byte/TYPE, #(aget ^bytes %1 %2), #(alength ^bytes %1), gen/byte))

(test/defspec short-array-roundtrip repetitions
  (array-roundtrip Short/TYPE, #(aget ^shorts %1 %2), #(alength ^shorts %1), short-gen))

(test/defspec int-array-roundtrip repetitions
  (array-roundtrip Integer/TYPE, #(aget ^ints %1 %2), #(alength ^ints %1), int-gen))

(test/defspec long-array-roundtrip repetitions
  (array-roundtrip Long/TYPE, #(aget ^longs %1 %2), #(alength ^longs %1), long-gen))

(test/defspec float-array-roundtrip repetitions
  (array-roundtrip Float/TYPE, #(aget ^floats %1 %2), #(alength ^floats %1), float-gen))

(test/defspec double-array-roundtrip repetitions
  (array-roundtrip Double/TYPE, #(aget ^doubles %1 %2), #(alength ^doubles %1), double-gen))



(defn same-array2d-values?
  [get-fn, length-fn, ^objects a, ^objects b]
  (let [n (alength a)]
    (loop [i 0]
      (if (< i n)
        (let [a_i (aget a i),
              b_i (aget b i)]
          (if (and
                (= (length-fn a_i) (length-fn b_i))
                (same-array-values? get-fn, length-fn, a_i, b_i))
            (recur (unchecked-inc i))
            false))
        true))))


(defn array2d-roundtrip
  [type, get-fn, length-fn, element-gen]
  (prop/for-all [v (gen/vector (gen/vector element-gen))]
    (let [^objects a (into-array (mapv #(into-array type, %) v)),
          ^objects b (-> a qf/quick-byte-freeze qf/quick-byte-defrost)]
      (and
        (=   (class a)   (class b))
        (= (alength a) (alength b))
        (same-array2d-values? get-fn, length-fn, a, b)))))


(test/defspec byte-array2d-roundtrip repetitions
  (array2d-roundtrip Byte/TYPE, #(aget ^bytes %1 %2), #(alength ^bytes %1), gen/byte))

(test/defspec short-array2d-roundtrip repetitions
  (array2d-roundtrip Short/TYPE, #(aget ^shorts %1 %2), #(alength ^shorts %1), short-gen))

(test/defspec int-array2d-roundtrip repetitions
  (array2d-roundtrip Integer/TYPE, #(aget ^ints %1 %2), #(alength ^ints %1), int-gen))

(test/defspec long-array2d-roundtrip repetitions
  (array2d-roundtrip Long/TYPE, #(aget ^longs %1 %2), #(alength ^longs %1), long-gen))

(test/defspec float-array2d-roundtrip repetitions
  (array2d-roundtrip Float/TYPE, #(aget ^floats %1 %2), #(alength ^floats %1), float-gen))

(test/defspec double-array2d-roundtrip repetitions
  (array2d-roundtrip Double/TYPE, #(aget ^doubles %1 %2), #(alength ^doubles %1), double-gen))


(def uuid-gen
  (gen/fmap #(UUID. (first %) (second %)) (gen/vector long-gen 2)))


(test/defspec uuid-roundtrip repetitions
  (prop/for-all [id uuid-gen]
    (-> id qf/quick-byte-freeze qf/quick-byte-defrost (= id))))



(def set-any-gen
  (gen/fmap set (gen/vector gen/simple-type)))


(test/defspec set-roundtrip repetitions
  (prop/for-all [s set-any-gen]
    (-> s qf/quick-byte-freeze qf/quick-byte-defrost (= s))))



; TODO: write generators to cover all supported types (arrays, ratios, sets, symbols, uuids) cf. frost.serializers

; nested types of gen/any are vectors, lists and maps (why not sets?)
(defn+opts roundtrip-anything-property
  [| :as options]
  (prop/for-all [x gen/any]
    (-> x (qf/quick-byte-freeze options) (qf/quick-byte-defrost options) (= x))))


(test/defspec roundtrip-anything {:num-tests repetitions}
  (roundtrip-anything-property))


(test/defspec roundtrip-anything-snappy-compression {:num-tests repetitions, :max-size 100}
  (roundtrip-anything-property :compressed true, :compression-algorithm :snappy))


(test/defspec roundtrip-anything-gzip-compression {:num-tests repetitions, :max-size 100}
  (roundtrip-anything-property :compressed true, :compression-algorithm :gzip))
