; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns frost.analysis
  (:import
    (com.esotericsoftware.kryo Kryo Serializer)
    (com.esotericsoftware.kryo.io Input Output))
  (:require
    [clojure.pprint :as pp]
    [swing.treetable :as tt]))


(defprotocol ISerializationStatistic
  (get-child-statistic [this, class, type])
  (add-bytes [this, byte-count])
  (get-children [this]))


(declare create-serialization-statistic)

(defn short-type-name
  [x]
  (if (class? x)
    (.getSimpleName ^Class x)
    (str x)))

(defn long-type-name
  [x]
  (if (class? x)
    (.getCanonicalName ^Class x)
    (str x)))

(deftype SerializationStatistic [my-class, my-type, total-bytes, object-count, children-map, children-order]
  
  ISerializationStatistic
  
  (get-child-statistic [this, class, type]
    (let [key [class type]]
      (if-let [statistic (get @children-map key)]
        statistic
        (let [statistic (create-serialization-statistic class type)]
          (swap! children-map assoc key statistic)
          (swap! children-order conj key)
          statistic))))
  
  (add-bytes [this, byte-count]
    (swap! total-bytes + byte-count)
    (swap! object-count inc)
    nil)
  
  (get-children [this]
    (for [key @children-order] 
      (get @children-map key)))
  
  
  clojure.lang.ILookup
  
  (valAt [this, key]
    (.valAt this, key, nil))
  
  (valAt [this, key, notFound]
    (or
      (case key
        :class my-class,
        :type  my-type,
        :total @total-bytes,
        :count @object-count,
        :avg   (if (pos? @object-count) (quot @total-bytes @object-count) 0),
        nil)
      notFound))
  
  Object
  (toString [this]
    (with-out-str
      (pp/pprint
	      (merge 
	        (into {} (for [k [:class :type :total :count :avg]] [k (get this k)]))
	        {:children (get-children this)}))))
  
  tt/ITreeTableNode
  
  (IsLeaf [this] (empty? @children-map))
  
  (GetChildCount [this]
    (count @children-map))
  
  (GetChild [this, index]
    (get @children-map (nth @children-order index)))
  
  (GetValueAt [this, column]
    (case (int column)
      0 (format "%s, %s" (short-type-name (:class this)) (short-type-name (:type this)))
      1 (format "%,d"    (:total this))
      2 (format "%,d"    (:count this))
      3 (format "%,d"    (:avg this))
      4 (long-type-name  (:class this))
      5 (long-type-name  (:type this))
      "")))

(defn create-serialization-statistic
  [class, type]
  (SerializationStatistic. class, type, (atom 0), (atom 0), (atom {}), (atom [])))




(def ^:dynamic *root-statistics* (atom {}))

(def ^:dynamic *parent-statistic* nil)


(defn get-my-statistic
  [class, type]
  (if *parent-statistic*
    (get-child-statistic *parent-statistic* class, type)
    (let [key [class type]]
      (if-let [statistic (get @*root-statistics* key)]
        statistic
        (let [statistic (create-serialization-statistic class, type)]
		      (swap! *root-statistics* assoc key statistic)
		      statistic)))))


(defn add-info
  [statistic-map, key, size]
  (-> statistic-map
    (update-in [key :count] (fnil inc 0))
    (update-in [key :total] (fnil + 0) size)))


(defn record-serialization
  [statistic-map, obj, size]
  (let [obj-type (type obj),
        obj-class (class obj)
        statistic-map (if (not= obj-class obj-type) (add-info statistic-map obj-type size) statistic-map)]
    (add-info statistic-map obj-class size)))


(defn ^Serializer analysis-serializer
  [^Serializer serializer]
  (proxy [Serializer] []
    (write [^Kryo kryo, ^Output out, obj]
      (let [prev-total (.total out),
            my-statistic (get-my-statistic (class obj), (type obj)),            
            _ (binding [*parent-statistic* my-statistic] (.write serializer kryo, out, obj)),
            total (.total out)]
        (add-bytes my-statistic (- total prev-total))))
    (read [^Kryo kryo, ^Input in, ^Class clazz]
      (.read serializer kryo, in, clazz))))


(defn determine-total-size
  [statistic-coll]
  (reduce #(+ % (:total %2)) 0 statistic-coll))


(def ^{:private true} column-specs 
  [(tt/->ColumnSpecification "Class & Type", 300, nil)  
   (tt/->ColumnSpecification  "Total Bytes", 120, (tt/create-int-cell-renderer))
   (tt/->ColumnSpecification        "Obj. Count",  80, (tt/create-int-cell-renderer))
   (tt/->ColumnSpecification   "AVG(Bytes)", 100, (tt/create-int-cell-renderer))
   (tt/->ColumnSpecification        "Class", 200, (tt/create-string-cell-renderer :left))
   (tt/->ColumnSpecification         "Type", 200, (tt/create-string-cell-renderer :left))])

(deftype RootNode [root-statistic-vec]
  tt/ITreeTableNode
  (IsLeaf [this] false)
  
  (GetChildCount [this]
    (count root-statistic-vec))
  
  (GetChild [this, index]
    (nth root-statistic-vec index))
  
  (GetValueAt [this, column]
    (cond 
      (= column 0) "Recorded Serializations"
      :else "")))

(defn show-analysis
  [statistics-map]
  (tt/show-tree-table 
    (RootNode. (->> statistics-map vals (sort-by :total >) vec)), 
    column-specs, "Serialization statistics", false, 1000, 600))


(defmacro with-serialization-analysis
  [& body]
 `(binding [*root-statistics* (atom {})]
    ~@body
    (show-analysis @*root-statistics*)))