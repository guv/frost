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


(defn illegal-argument
  [fmt, & args]
  (throw (IllegalArgumentException. (apply format fmt args))))


(defn resolve-fn
  "Retrieve function specified by the given symbol or string.
  This function is only thread-safe when txload is enabled previously."
  [symbol-str]
  (if (or (string? symbol-str) (symbol? symbol-str))    
    (let [symb (symbol symbol-str)
          symb-ns (namespace symb)]
      (if (nil? symb-ns) 
        (illegal-argument "Function symbol \"%s\" must have a full qualified namespace!" symb)
        (do
          ; load namespace if needed - require is thread safe thanks to txload (must be enabled at startup)
          (require (symbol symb-ns))
          ; resolve symbol
          (if-let [v (some-> symb resolve var-get)]
	           v
	           (illegal-argument "Function \"%s\" does not exist!" symb)))))
    (cond
      (ifn? symbol-str)
        symbol-str
      (nil? symbol-str)
        (illegal-argument "No function identifier given (argument is nil)!")
      :else
        (illegal-argument "resolve-fn expects a symbol or a string"))))