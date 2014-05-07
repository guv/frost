; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns frost.util
  (:import
    java.io.Closeable))


(defmacro conditional-wrap
  "Wraps the given body in the given form if the condition evaluates to true at runtime.
  Otherwise the body is executed normally.
  Example: (conditional-wrap lock? (locking this %) (.close out))"
  [condition, wrap-form, & body]
  (let [do-body `(do ~@body)
        wrapped-body (replace {'% do-body} wrap-form)]
    `(if ~condition
       ~wrapped-body
       ~do-body)))


(defn close
  "Closes the given Closeable instance omitting reflection."
  [^Closeable closeable]
  (.close closeable))