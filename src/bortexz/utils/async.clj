(ns bortexz.utils.async
  (:require [clojure.core.async :as a]
            [clojure.core.async.impl.protocols :as ap]
            [bortexz.utils.core :as uc])
  (:import
   (clojure.core.async.impl.channels ManyToManyChannel)))

(defn- default-ex-handler
  [ex]
  (-> (Thread/currentThread)
      .getUncaughtExceptionHandler
      (.uncaughtException (Thread/currentThread) ex))
  nil)

(defn chan? [x] (instance? ManyToManyChannel x))

(defn interval
  "Creates a process that will put the result of calling `f` every `ms` milliseconds into `out`.
   The counter starts again once the value has ben put into the output chan.
   Closing the output chan will asynchronously stop the process.
   Returns `out`."
  [f ms out]
  (a/go-loop [timer (a/timeout ms)]
    (a/<! timer)
    (when (a/>! out (f))
      (recur (a/timeout ms))))
  out)

(defn debounce
  "Creates a process that will accumulate items read from `in` (with `opts.add-item`),
   and will put them into `out` iff `ms` milliseconds have ellapsed without any new value from `in`.
   
   Whenever `in` is closed, the process stops. If `opts.close?` is true, when the process finishes 
   it will close `out` ch. If `out` is closed from the outside, stops reading from `in`.
   
   Returns `out`.
   
   opts:
   - `add-item` 2-arity fn with current value and new item, returns next value. Defaults to `(fn [_ v] v)`
     (only keeps latest value). After the current accumulated value has been put into `out`, next `add-item`
     will have `nil` as value. Will not get called when reading nil due to `in` being closed.
   - `close?` if true, closes `out` when the process finishes.
   "
  ([ms in out] (debounce ms in out {}))
  ([ms in out {:keys [close? add-item]
               :or {close? true
                    add-item (fn [_ v] v)}}]
   (a/go
     (loop [val (add-item nil (a/<! in))
            in-closed? false]
       (let [ops (cond-> []
                   (not in-closed?) (conj in)
                   val (conj (a/timeout ms)))]
         (when (seq ops)
           (let [[v p] (a/alts! ops)]
             (if (= p in)
               (recur (if (some? v) (add-item val v) v) (nil? v))
               (do (a/>! out val)
                   (recur nil in-closed?)))))))
     (when close? (a/close! out)))
   out))

(defmacro <t!
  "Like <! with a `ms` timeout as first argument. Returns ::timeout if `ms` milliseconds ellapsed without
   success reading from `ch`."
  [ms ch]
  `(let [ch# ~ch]
     (a/alt! [ch# (a/timeout ~ms)] ([v# p#] (if (= p# ch#) v# ::timeout)))))

(defmacro >t!
  "Like >! with a `ms` timeout as first argument. Returns ::timeout if `ms` milliseconds ellapsed without
   success putting into `ch`."
  [ms ch val]
  `(let [ch# ~ch]
     (a/alt! [[ch# ~val] (a/timeout ~ms)] ([v# p#] (if (= p# ch#) v# ::timeout)))))

(defn <t!!
  "Like <!! with a `ms` timeout as first argument. Returns ::timeout if `ms` milliseconds ellapsed without
   success reading from `ch`."
  [ms ch]
  (a/alt!! [ch (a/timeout ms)] ([v p] (if (= p ch) v ::timeout))))

(defn >t!!
  "Like >!! with a `ms` timeout as first argument. Returns ::timeout if `ms` milliseconds ellapsed without
   success putting into `ch`."
  [ms ch val]
  (a/alt!! [[ch val] (a/timeout ms)] ([v p] (if (= p ch) v ::timeout))))

(defmacro <?
  "Like <!, but will throw the exception if the value read from `ch` is an exception"
  [ch]
  `(let [v# (a/<! ~ch)]
     (when (uc/exception? v#) (throw v#))
     v#))

(defn <??
  "Like <!, but will throw the exception if the value read from `ch` is an exception"
  [ch]
  (let [v (a/<!! ch)]
    (when (uc/exception? v) (throw v))
    v))

(defn bundle
  "Creates a process that adds items taken from input chan `in` into a `bundle` using `opts.add-item` fn. 
   The initial value of `bundle` can be specified with `opts.init`.

   When the bundle is ready (`opts.ready?` fn) and the `out` chan can accept new values, 
   the bundle is put into `out`, and resets its value to `opts.init`. If the bundle is ready, but 
   `out` can't accept new values, the process keeps taking items from `in` and adding them to the bundle, 
   unless bundle is full (`opts.full?` fn), in which case it only tries to put the bundle into `out`.

   When the `in` ch closes, the process will no longer try to take values from it, and instead will
   only try to put the current bundle into `out` iff it's ready. If not ready, then the process
   finishes.

   If `out` ch is found closed when trying to output the bundle, the process finishes.

   `ready?`, `full?` and `add-item` fns are called inside go-loop and should behave accordingly 
   (no i/o, no expensive computations, etc)

   Returns `out`.

   opts:
   - `init` the initial value of the bundle, will be used as a starting point, and reseted to
     this value when it gets sent over `out` chan.
   
   - `add-item` 2-arity fn ([bundle new-item]) that returns bundle with the new item added.
     When the `in` ch closes, add-item will also be called with `nil` as new-item, so the user can `mark`
     the bundle as ready, to allow the remaining contents of the bundle to be sent before finishing the process.
   
   - `ready?` 1-arity fn ([bundle]) that returns logical true if the bundle is ready to be sent to `out`
   
   - `full?` 1-arity fn ([bundle]) that returns logical true if the bundle can't be added new values. 
   Note that full? is only checked after ready? returns true. Defaults to `(constantly false)`, 
   creating an unbounded bundle.

   - `close?` if true, the out ch will be closed after the process finishes. Defaults to true.
   "
  [in out {:keys [init add-item ready? full? close?]
           :or {full? (constantly false) close? true}}]
  (assert (and in out) ":in and :out chans must be specified")
  (assert (not (full? init)) "The init value cannoty already be full")
  (assert (not (ready? init)) "The init value cannoty already be ready")
  (let [alts-ops (fn [b in-closed?]
                   (let [ready? (ready? b)
                         full? (and ready? (full? b))]
                     (cond-> []
                       (and (not in-closed?) (not full?)) (conj in)
                       ready? (conj [out b]))))]
    (a/go-loop [bundle init
                in-closed? false
                ops (alts-ops bundle in-closed?)]
      (if (seq ops)
        (let [[v p] (a/alts! ops)]
          (condp = p
            in (let [new-b (add-item bundle v)
                     closed? (nil? v)]
                 (recur new-b closed? (alts-ops new-b closed?)))
            out (when v
                  (recur init in-closed? (alts-ops init in-closed?)))))
        (when close? (a/close! out))))
    out))


(defn broadcast
  "Puts `val` into all `chs` in parallel. Returns a ch that will contain a collection of the `chs`
   that were found closed while trying to put `val` into them, when all chans have accepted the item.
   
   Easier than creating a mult for just distributing one value."
  {:copyright "Rich Hickey, since this is a modified version of core.async/mult internal distribution mechanism"}
  [val chs]
  (let [chs    chs
        dch    (a/chan 1)
        c      (atom (count chs))
        closed (atom [])
        done   (fn [_#]
                 (when (zero? (swap! c dec))
                   (a/put! dch @closed)
                   (a/close! dch)))]
    (run! (fn [ch]
            (when-not (a/put! ch val done) (swap! closed conj ch)))
          chs)
    dch))

(defn- fworker-go
  [in ex-handler]
  (a/go-loop []
    (when-let [f (a/<! in)]
      (try
        (let [res (f)]
          (when (chan? res) (a/<! res)))
        (catch Throwable t (ex-handler t)))
      (recur))))

(defn- fworker-thread
  [in ex-handler]
  (a/thread
    (loop []
      (when-let [f (a/<!! in)]
        (try
          (let [res (f)]
            (when (chan? res) (a/<!! res)))
          (catch Throwable t (ex-handler t)))
        (recur)))))

(defn fworker
  "Creates a worker that reads nullary fn's from `in` and executes them. if the fn returns a
   chan, the worker waits for the ch to emit/close before trying to take more fns from `in`.
   
   Creates a `go-loop` by default, but can be changed to `thread loop` with `opts.thread?`.
   
   When closing `in`, the process will finish. Returns `ch` that closes when the worker has finished.
   
   opts:
   - `thread?` Creates the worker using async/thread
   - `ex-handler` exception handler to be used if incoming `fn` throws"
  ([in] (fworker in {}))
  ([in {:keys [thread? ex-handler] :or {thread? false ex-handler default-ex-handler}}]
   (if-not thread?
     (fworker-go in ex-handler)
     (fworker-thread in ex-handler))))

(defn fworkers
  "Creates `n` workers that run in parallel. See [[worker]]. 
   Returns a `ch` that closes when all workers have finished."
  ([n in] (fworkers n in {}))
  ([n in opts]
   (let [xs (mapv (fn [_] (fworker in opts)) (range n))]
     (a/go (doseq [x xs] (a/<! x))))))

(defn pipe-process
  "Same as core.async/pipe, but returns the internal go-loop ch instead of `to`"
  {:copyright "Rich Hickey, as this is a modified version of core.async/pipe"}
  ([from to] (pipe-process from to true))
  ([from to close?]
   (a/go-loop []
     (let [v (a/<! from)]
       (if (nil? v)
         (when close? (a/close! to))
         (when (a/>! to v)
           (recur)))))))

(defn- consume-go
  [in f ex-handler]
  (a/go-loop []
    (when-let [v (a/<! in)]
      (try
        (let [res (f v)] (when (chan? res) (a/<! res)))
        (catch Throwable t (ex-handler t)))
      (recur))))

(defn- consume-thread
  [in f ex-handler]
  (a/go-loop []
    (when-let [v (a/<! in)]
      (try
        (let [res (f v)] (when (chan? res) (a/<! res)))
        (catch Throwable t (ex-handler t)))
      (recur))))

(defn consume
  "Creates a process that takes items from `in` and executes `f` with each item.
   If `f` returns a ch, the internal process will park/block waiting for the ch to emit/close, ignoring
   its value. Returns a chan that will close when in is exhausted.

   Accepts an optional 3 argument `opts`:
   - `thread?` If the internal process should use a thread. Defaults to false (uses go-loop)
   - `ex-handler` Exception handler if `f` throws an exception. Defaults to use "
  ([in f] (consume-go in f {}))
  ([in f {:keys [thread? ex-handler] :or {thread? false ex-handler default-ex-handler}}]
   (if thread?
     (consume-thread in f ex-handler)
     (consume-go in f ex-handler))))

(defn mult
  "Alternative implementation of core.async/mult with extra `opts`.
   
   opts:
   - `events-ch` optional ch where to receive events related to the mult. Current events are:
     - :on-fill Sent when the mult goes from having 0 taps to at least one tap.
     - :on-empty Sent when the mult goes from having at least one tap to 0 taps.
     - the ch will be closed when the mult process finishes.
     An internal sliding-buffer is created that pipes to this channel, meaning that older events might get dropped in
     favor of later events.
   
   - `finished-close?` If tapping into a finished mult with `close?` true, then it automatically
     closes the tap ch. Defaults to true. If set to false, same behaviour as core.async/mult
     (doesn't close taps when added after main go-loop has finished)."
  {:copyright "Rich Hickey, since this code started off from the original core.async/mult impl."}
  ([ch] (mult ch {}))
  ([ch {:keys [events-ch finished-close?] :or {finished-close? true}}]
   (assert (or (not events-ch) (chan? events-ch)) "If events is specified, it must be a chan")
   (let [chs_ (atom #{})
         ch->close?_ (atom {})
         closed?_ (atom false)
         fx!__ (atom nil)

         sliding-events (a/chan (a/sliding-buffer 1))

         do-events (fn [[prev-chs new-chs]]
                     (when events-ch
                       (cond
                         (and (empty? prev-chs) (seq new-chs))
                         (a/put! sliding-events :on-fill)

                         (and (seq prev-chs) (empty? new-chs))
                         (a/put! sliding-events :on-empty))))

         untap-chs (fn [chs]
                     (uc/chain-fx!
                      fx!__
                      (fn [_]
                        (do-events (swap-vals! chs_ #(reduce disj % chs)))
                        (swap! ch->close?_ #(reduce dissoc % chs)))))

         m (reify
             a/Mux
             (muxch* [_] ch)

             a/Mult
             (tap* [_ ch close?]
               (uc/chain-fx!
                fx!__
                (fn [_]
                  (if @closed?_
                    (when (and finished-close? close?)
                      (a/close! ch))
                    (do
                      (do-events (swap-vals! chs_ conj ch))
                      (swap! ch->close?_ assoc ch close?))))))

             (untap* [_ ch] (untap-chs [ch]))

             (untap-all* [_]
               (uc/chain-fx! fx!__ (fn [_]
                                     (do-events (swap-vals! chs_ empty))
                                     (swap! ch->close?_ empty)))))

         ;; Close mult
         close-mult (fn []
                      (uc/chain-fx!
                       fx!__
                       (fn [_]
                         (reset! closed?_ true)
                         (run! (fn [[ch close?]] (when close? (a/close! ch))) @ch->close?_)
                         (a/close! sliding-events))))

         ;; broadcast
         dchan (a/chan 1)
         dctr (atom nil)
         done (fn [_] (when (zero? (swap! dctr dec))
                        (a/put! dchan true)))
         untap (volatile! [])]
     
     (when events-ch (a/pipe sliding-events events-ch))

     (a/go-loop []
       (let [val (a/<! ch)]
         (if (nil? val)
           (close-mult)
           (let [chs @chs_]
             (when (seq chs)
               (vreset! untap [])
               (reset! dctr (count chs))
               (run! (fn [c]
                       (when-not (a/put! c val done)
                         (vswap! untap conj c)))
                     chs)
               (when (seq @untap) (untap-chs @untap))
               (a/<! dchan))
             (recur)))))
     m)))

(defn pub
  "Alternative implementation of core.async/pub with extra `opts`.

   opts:
   - `buf-fn` Like `buf-fn` in core.async/pub. Fn accepting topic that returns the buffer to be used
     per topic.
      
   - `events-ch` chan that contains events related to each topic (coming from the internal mult of each
     topic). Supported events are:
     - [:on-fill <topic>] Sent when topic goes from having no subscribers to at least one subscriber.
     - [:on-empty <topic>] Sent when topic goes from having subscribers to not having any subscribers.
     - Will be closed when the pub has finished and all internal topic mults are finished.
     Each topic uses a sliding-buffer of events, some events might get dropped in favor of a later status of the topic.

   - `finished-close?` See `finished-close?` in [[mult]]
   
   Notes:
   - Once the ch for a topic is created, it will remain in the pub and not be removed/closed. For this
   reason, the number of topics should be bounded."
  {:copyright "Rich Hickey, since this code started off from the original core.async/pub impl."}
  ([ch topic-fn] (pub ch topic-fn nil))
  ([ch topic-fn {:keys [buf-fn events-ch finished-close?]
                 :or {buf-fn (constantly nil)
                      finished-close? true}}]
   (let [mults_ (atom {}) ; topic->mult
         open_ (atom #{}) ; used to close events-ch (if specified) when all mults closed
         fx!__ (atom nil)

         ensure-mult (fn [topic]
                       (or (get @mults_ topic)
                           (uc/chain-fx!
                            fx!__
                            (fn [_]
                              (if-let [m (get @mults_ topic)]
                                m
                                (let [buf-ch (a/chan (buf-fn topic))
                                      ech (when events-ch (a/chan 1 (map (fn [ev] [ev topic]))))
                                      m (mult buf-ch
                                              (cond-> {:finished-close? finished-close?}
                                                ech (assoc :events-ch ech)))]
                                  (when ech
                                    (swap! open_ conj topic)
                                    (a/go (a/<! (pipe-process ech events-ch false))
                                          (when (empty? (swap! open_ disj topic))
                                            (a/close! events-ch))))
                                  (swap! mults_ assoc topic m)
                                  m))))))

         p (reify
             a/Mux
             (muxch* [_] ch)

             a/Pub
             (sub* [_ topic ch close?]
               (a/tap* (ensure-mult topic) ch close?))
             (unsub* [_ topic ch]
               (when-let [m (get @mults_ topic)] (a/untap* m ch)))
             (unsub-all* [_ topic]
               (when-let [m (get @mults_ topic)] (a/untap-all* m)))
             (unsub-all* [_]
               (run! #(a/untap-all* %) (vals @mults_))))]

     (a/go-loop []
       (let [val (a/<! ch)]
         (if (nil? val)
           (run! #(a/close! %) (map a/muxch* (vals @mults_)))
           (let [topic (topic-fn val)
                 m (get @mults_ topic)]
             (when m (a/>! (a/muxch* m) val))
             (recur)))))
     p)))

(defn spread-pub
  "Creates a new pub wrapping pub `p`. You can specify `opts.chan-fn` to create a source ch for a given topic, 
   allowing you to specify a custom xf that will be applied only once for all subscribed chs of a given topic.
   
   Additionally, this internal mult will sub automatically to the underlying `p` when there are 
   any taps on a given topic, and will automatically unsub from `p` when there aren't taps on it. This is done
   asynchronously, on an internal go-loop that reads from the events emitted by each [[mult]].
   
   Compatible with both core.async/pub and [[pub]]."
  ([p] (spread-pub p {}))
  ([p {:keys [chan-fn finished-close?] :or {chan-fn (fn [_topic] (a/chan))
                                            finished-close? true}}]
   (let [mults_ (atom {}) ; topic->mult
         open_  (atom #{})
         fx!__ (atom nil)

         events-ch (a/chan 128)

         ensure-mult (fn [topic]
                       (or (get @mults_ topic)
                           (uc/chain-fx!
                            fx!__
                            (fn [_]
                              (if-let [m (get @mults_ topic)]
                                m
                                (let [mch (chan-fn topic)
                                      ech (a/chan 1 (map (fn [ev] [ev topic])))
                                      m (mult mch {:events-ch ech :finished-close? finished-close?})]
                                  (swap! open_ conj topic)
                                  (a/go (a/<! (pipe-process ech events-ch false))
                                        (when (empty? (swap! open_ disj topic))
                                          (a/close! events-ch)))
                                  (swap! mults_ assoc topic m)
                                  m))))))]

     (a/go-loop []
       (when-let [v (a/<! events-ch)]
         (let [[ev t] v
               ch (a/muxch* (get @mults_ t))]
           (case ev
             :on-fill (a/sub* p t ch true)
             :on-empty (a/unsub* p t ch))
           (recur))))

     (reify
       a/Mux
       (muxch* [_] (a/muxch* pub))

       a/Pub
       (sub* [_ topic ch close?]
         (let [m (ensure-mult topic)]
           (a/tap* m ch close?)))
       (unsub* [_ topic ch]
         (when-let [m (get @mults_ topic)] (a/untap* m ch)))
       (unsub-all* [_ topic]
         (when-let [m (get @mults_ topic)] (a/untap-all* m)))
       (unsub-all* [_]
         (run! #(a/untap-all* %) (map a/muxch* (vals @mults_))))))))

(defn profile-buf
  "Wraps `buf` into a new buffer that works exactly like `buf` (calls protocol fns of wrapped buf), 
   with the addition of calling `pfn` with the remaining number of itmes in the buffer when taking
   values out of the buffer.
   
   Depends on internal implementation details of core.async, use at your own risk!"
  [buf pfn]
  (reify
    ap/Buffer
    (full? [_] (ap/full? buf))
    (remove! [_] (let [i (ap/remove! buf)] (pfn (.count buf)) i))
    (add!* [_ itm] (ap/add!* buf itm))
    (close-buf! [_] (ap/close-buf! buf))

    clojure.lang.Counted
    (count [_] (.count buf))))

(defn arrange-by
  "Starts a process that will read values from all `chs`, and puts them in `out` in order determined
   by comparing (keyfn item) using defualt comparator, or a custom supplied one as `opts.comparator`

   The process waits until having values from all ch's before emitting a value.
   (i.e when the value from a chan is emitted, the process waits until the same `ch` emits a new value 
   to compare again with the cached vals).
   
   If multiple items have same result of `keyfn`, then the first one that was taken is emitted.

   When all chs are closed, the process finishes. `opts.close?` can be used to close `out` when this
   happens.

   Example use case: Reorder different historical data sources by :timestamp"
  ([keyfn chs out] (arrange-by keyfn chs out {}))
  ([keyfn chs out {:keys [close? comparator] :or {close? true comparator compare}}]
   (let [cwrap (fn [[a an] [b bn]]
                 (let [c (comparator a b)]
                   (if (zero? c)
                     (compare an bn)
                     c)))]
     (a/go-loop [take-chs (set chs)
                 queued-chs #{}
                 q (sorted-map-by cwrap)
                 counter 0]
       (if (seq take-chs)
         (let [ch (first take-chs)]
           (if-let [v (a/<! ch)]
             (recur (disj take-chs ch)
                    (conj queued-chs ch)
                    (assoc q [(keyfn v) counter] [ch v])
                    (inc counter))
             (recur (disj take-chs ch) queued-chs q counter)))
         (if (seq q)
           (let [[k [ch v]] (first q)]
             (a/>! out v)
             (recur (conj take-chs ch)
                    (disj queued-chs ch)
                    (dissoc q k)
                    (inc counter)))
           (when close? (a/close! out))))))))
