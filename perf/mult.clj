(ns mult
  (:require [criterium.core :as c]
            [clojure.core.async :as a]
            [bortexz.utils.async :as ua]))

(defn setup-mult
  [multf src-buf tap-n]
  (let [src (a/chan src-buf)
        m (multf src)
        taps (mapv (fn [_]
                     (let [ch (a/chan (a/dropping-buffer 1))]
                       (a/tap m ch)))
                   (range tap-n))]
    [src m taps]))

(defn setup-taps-done [taps]
  (let [ctr  (atom (count taps))
        dch  (a/chan 1)
        done (fn [_] (when (zero? (swap! ctr dec))
                       (a/put! dch true)))]
    (run! (fn [c] (a/take! c done)) taps)
    dch))

(comment
  ;; Tests done in a Macbook Air M1 with 8Gb ram
  ;; 1 tap
  (let [[src _ taps] (setup-mult a/mult nil 1)]
    (def core-src src))

  (let [[src _ taps] (setup-mult ua/mult nil 1)]
    (def utils-src src))

  ; (def taps-done (setup-taps-done taps))

  (c/quick-bench (a/<!! (a/onto-chan! core-src (range 1000) false))) ; => 1.50ms
  (c/quick-bench (a/<!! (a/onto-chan! utils-src (range 1000) false))) ; => 1.24ms (~1.2x faster)

  ;; 500 taps
  (let [[src _ taps] (setup-mult a/mult nil 500)]
    (def core-src src))

  (let [[src _ taps] (setup-mult ua/mult nil 500)]
    (def utils-src src))

  ; (def taps-done (setup-taps-done taps))

  (c/quick-bench (a/<!! (a/onto-chan! core-src (range 1000) false))) ; => 143ms
  (c/quick-bench (a/<!! (a/onto-chan! utils-src (range 1000) false))) ; => 37ms (!! ~3.8x faster) 
  )
