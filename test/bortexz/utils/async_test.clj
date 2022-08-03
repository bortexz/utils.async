(ns bortexz.utils.async-test
  (:require [clojure.test :refer [deftest testing is]]
            [bortexz.utils.async :as ua :refer [<t!! >t!!]]
            [clojure.core.async :as a]))

(def ^:private wait 20)

(deftest bundle-process-test
  (let [opts {:init []
              :add-item (fn [b i] (cond-> b i (conj i)))
              :ready? (fn [b] (> (count b) 1))
              :full? (fn [b] (> (count b) 2))}]
    (testing "Base test"
      (let [in (a/chan)
            out (ua/bundle in (a/chan) opts)]
        (is (true? (>t!! wait in 1)) "Can put the first value, process is accepting items")
        (is (true? (>t!! wait in 2)) "Can put a second value into in, keeps accepting items")
        (is (= [1 2] (<t!! wait out)) "Reads the correct value from out")
        (a/close! in)
        (is (nil? (<t!! wait out)) "Closes out when in chan is closed")))

    (testing "Can only take the bundle from out whenever it's ready"
      (let [in (a/chan)
            out (ua/bundle in (a/chan) opts)]
        (>t!! wait in 1)
        (is (#{::ua/timeout} (<t!! wait out)) "Reading from out when bundle is not ready timeouts")
        (>t!! wait in 2)
        (is (= [1 2] (<t!! wait out)) "Reads the correct value from out")
        (a/close! in)))

    (testing "No longer reads from in when bundle is full"
      (let [in (a/chan)
            out (ua/bundle in (a/chan) opts)]
        (>t!! wait in 1)
        (>t!! wait in 2)
        (>t!! wait in 3)
        (is (#{::ua/timeout} (>t!! wait in 4)) "bundle is full, can't input more values")
        (is (= [1 2 3] (<t!! wait out)) "Reads the correct value from out")
        (a/close! in)))

    (testing "Even after in is closed, attemps to put bundle in out if ready"
      (let [in (a/chan)
            out (ua/bundle in (a/chan) opts)]
        (>t!! wait in 1)
        (>t!! wait in 2)
        (a/close! in)
        (is (= [1 2] (<t!! wait out)) "bundle is full, can't input more values")
        (is (nil? (<t!! wait out)) "Out is closed after the final bundle is sent")))))

(deftest debounce-test
  (testing "Base test"
    (let [in (a/chan)
          out (ua/debounce 100 in (a/chan))]
      (is (true? (>t!! wait in 1)) "Takes val from in")
      (is (true? (>t!! wait in 1)) "Can put a second value")
      (is (#{::ua/timeout} (<t!! wait out)) "It is not immediately available in out")
      (a/<!! (a/timeout 100))
      (is (= 1 (<t!! wait out)) "Second val is available after the debounce time has passed")))

  (testing "When there's a value pending and in closes, it still waits for debounce, puts it into out and closes out"
    (let [in (a/chan)
          out (ua/debounce 100 in (a/chan))]
      (is (true? (>t!! wait in 1)) "Takes val from in")
      (is (true? (>t!! wait in 2)) "Can put a second value")
      (is (#{::ua/timeout} (<t!! wait out)) "It is not immediately available in out")
      (a/<!! (a/timeout 100))
      (is (= 2 (<t!! wait out)) "Second val is available after the debounce time has passed")))

  (testing "when add-item specified, gets called to accumulate values"
    (let [in (a/chan)
          out (ua/debounce 100 in (a/chan) {:add-item (fnil conj [])})]
      (is (true? (>t!! wait in 1)) "Takes val from in")
      (is (true? (>t!! wait in 2)) "Can put a second value")
      (a/<!! (a/timeout 100))
      (is (= [1 2] (<t!! wait out)) "Second val is available after the debounce time has passed"))))

(deftest broadcast-test
  (testing "puts to all chs"
    (let [ch1 (a/chan 1)
          ch2 (a/chan 1)
          bc  (ua/broadcast true [ch1 ch2])]
      (is (true? (<t!! wait ch1)) "ch1 contains val")
      (is (true? (<t!! wait ch2)) "ch2 contains val")))

  (testing "doesn't finish until all chans accept"
    (let [ch1 (a/chan)
          ch2 (a/chan 1)
          bc  (ua/broadcast true [ch1 ch2])]
      (is (= ::ua/timeout (<t!! wait bc)) "Times out as ch1 hasn't accepted yet")
      (is (true? (<t!! wait ch1)) "unbuffered ch1 has value from broadcast")
      (is (not= :timeout (<t!! wait bc)) "Finishes now that ch1 has accepted val")))

  (testing "correctly returns closed chs"
    (let [ch1 (a/chan 1)
          ch2 (a/chan 1)
          _   (a/close! ch1)
          bc  (ua/broadcast true [ch1 ch2])]
      (is (= [ch1] (<t!! wait bc)) "contains closed ch1"))))

(deftest fworker-test
  (testing "Executes functions sent"
    (let [v (atom nil)
          fch (a/chan)
          wch (ua/fworker fch)]
      (>t!! wait fch (fn [] (reset! v true)))
      (a/close! fch)
      (is (nil? (<t!! wait wch)))
      (is (true? @v))))

  (testing "Functions returning chs works properly"
    (let [v (atom nil)
          fch (a/chan)
          wch (ua/fworker fch)]
      (>t!! wait fch (fn []
                       (a/go (a/<! (a/timeout wait))
                             (reset! v true))))
      (a/close! fch)
      (is (nil? (<t!! (* 2 wait) wch))) ; Sometimes doesn't work with * 2 wait, unsure why is that slow.
      (is (true? @v))))

  (testing "Ex handler captures exceptions"
    (let [v (atom nil)
          fch (a/chan)
          wch (ua/fworker fch {:ex-handler (fn [e] (reset! v e))})]
      (>t!! wait fch (fn [] (throw (ex-info "" {}))))
      (a/close! fch)
      (is (nil? (<t!! wait wch))) ; double waiting as timeout is `wait`
      (is (instance? clojure.lang.ExceptionInfo @v)))))

(deftest consume-test
  (testing "Executes f with items"
    (let [v (atom nil)
          ch (a/chan)
          c (ua/consume ch (fn [i] (reset! v i)))]
      (>t!! wait ch true)
      (a/close! ch)
      (is (nil? (<t!! wait c)))
      (is (true? @v))))

  (testing "f returning chs works properly"
    (let [v (atom nil)
          ch (a/chan)
          c (ua/consume ch (fn [i]
                             (a/go (a/<! (a/timeout wait))
                                   (reset! v i))))]
      (>t!! wait ch true)
      (a/close! ch)
      (is (nil? (<t!! (* 2 wait) c))) ; double waiting as timeout is `wait`
      (is (true? @v))))

  (testing "Ex handler captures exceptions"
    (let [v (atom nil)
          ch (a/chan)
          c (ua/consume ch
                        (fn [i] (throw (ex-info "" {})))
                        {:ex-handler (fn [e] (reset! v e))})]
      (>t!! wait ch true)
      (a/close! ch)
      (is (nil? (<t!! wait c))) ; double waiting as timeout is `wait`
      (is (instance? clojure.lang.ExceptionInfo @v)))))

(deftest tap-test
  (testing "Passes core.async/mult tests"
    (let [a (a/chan 4)
          b (a/chan 4)
          src (a/chan)
          m (ua/mult src)]
      (a/tap m a)
      (a/tap m b)
      (a/pipe (a/to-chan! (range 4)) src)
      (is (= [0 1 2 3]
             (<t!! wait (a/into [] a))))
      (is (= [0 1 2 3]
             (<t!! wait (a/into [] b)))))

      ;; ASYNC-127
    (let [ch (a/to-chan! [1 2 3])
          m (ua/mult ch)
          t-1 (a/chan)
          t-2 (a/chan)
          t-3 (a/chan)]
      (a/tap m t-1)
      (a/tap m t-2)
      (a/tap m t-3)
      (a/close! t-3)
      (is (= 1 (<t!! wait t-1)))
      (is (= nil (a/poll! t-1)))
      (is (= 1 (<t!! wait t-2)))
      (is (= 2 (<t!! wait t-1)))
      (is (= nil (a/poll! t-1)))))

  (testing "events ch"
    (let [ch1 (a/chan 1)
          ev (a/chan 2)
          src (a/chan)
          m (ua/mult src {:events-ch ev})]
      (a/tap m ch1)
      (>t!! wait src true)
      (a/untap m ch1)
      (a/close! src)
      (is (= :on-fill (<t!! wait ev)))
      (is (= :on-empty (<t!! wait ev)))
      (is (nil? (<t!! wait ev)))))

  (testing "events ch: when taps are found closed it will also send :on-empty"
    (let [ch1 (a/chan 1)
          ev (a/chan 2)
          src (a/chan)
          m (ua/mult src {:events-ch ev})]
      (a/tap m ch1)
      (a/close! ch1)
      (>t!! wait src true)
      (a/close! src)
      (is (= :on-fill (<t!! wait ev)))
      (is (= :on-empty (<t!! wait ev)))
      (is (nil? (<t!! wait ev)))))

  (testing "close-tap-on-finished?"
    (let [ch1 (a/chan)
          ev  (a/chan 1)
          src (a/chan)
          m (ua/mult src {:events-ch ev})]
      (a/close! src)
      (is (nil? (<t!! wait ev)))
      (a/tap m ch1 true)
      (is (nil? (<t!! wait ch1))))))

(deftest pub-test
  (testing "Passes core.async/pub tests"
    (let [a-ints (a/chan 5)
          a-strs (a/chan 5)
          b-ints (a/chan 5)
          b-strs (a/chan 5)
          src (a/chan)
          p (ua/pub src (fn [x]
                          (if (string? x)
                            :string
                            :int)))]
      (a/sub p :string a-strs)
      (a/sub p :string b-strs)
      (a/sub p :int a-ints)
      (a/sub p :int b-ints)
      (a/pipe (a/to-chan! [1 "a" 2 "b" 3 "c"]) src)
      (is (= [1 2 3]
             (<t!! wait (a/into [] a-ints))))
      (is (= [1 2 3]
             (<t!! wait (a/into [] b-ints))))
      (is (= ["a" "b" "c"]
             (<t!! wait (a/into [] a-strs))))
      (is (= ["a" "b" "c"]
             (<t!! wait (a/into [] b-strs))))))

  (testing "events ch"
    (let [ch1 (a/chan 1)
          ev (a/chan 1)
          src (a/chan)
          p (ua/pub src first {:events-ch ev})]
      (a/sub p :topic ch1)
      (is (= [:on-fill :topic] (<t!! wait ev)))
      (a/unsub p :topic ch1)
      (a/close! src)
      (is (= [:on-empty :topic] (<t!! wait ev)))
      (is (nil? (<t!! wait ev))))))

(deftest spread-pub-test
  (testing "spread-pub-test works"
    (let [src-ch (a/chan)
          ech (a/chan 3)
          internal-pub (ua/pub src-ch first {:events-ch ech})
          spub (ua/spread-pub internal-pub)
          sub-ch (a/chan)]
      (a/sub spub :topic-a sub-ch)
      (is (= [:on-fill :topic-a] (<t!! wait ech)))
      (>t!! wait src-ch [:topic-a 1])
      (is (= [:topic-a 1] (<t!! wait sub-ch)))
      (a/unsub spub :topic-a sub-ch)
      (is (= [:on-empty :topic-a] (<t!! wait ech))))))

(deftest profile-buf-test
  (testing "Returns remaining number of items in buf"
    (let [p (atom [])
          ch (a/chan (ua/profile-buf (a/buffer 3) (fn [i] (swap! p conj i))))]
      (<t!! wait (a/onto-chan! ch [1 2 3]))
      (<t!! wait (ua/consume ch (fn [_])))
      (is (= [2 1 0] @p)))))

(deftest arrange-by-test
  (testing "basic test"
    (let [ch1 (a/chan 1)
          ch2 (a/chan 1)
          out (a/chan 5)
          ps  (ua/arrange-by identity [ch1 ch2] out)]
      (>t!! wait ch1 1)
      (>t!! wait ch2 2)
      (>t!! wait ch1 3)
      (>t!! wait ch1 4)
      (>t!! wait ch2 5)
      (a/close! ch1)
      (a/close! ch2)
      (is (= [1 2 3 4 5] (<t!! wait (a/into [] out))))))

  (testing "equal f val are sent in order received"
    (let [ch1 (a/chan 1)
          ch2 (a/chan 1)
          out (a/chan 3)
          ps  (ua/arrange-by first [ch1 ch2] out)]
      (>t!! wait ch1 [1 ch1])
      (>t!! wait ch2 [2 ch2])
      (>t!! wait ch1 [2 ch1])
      (a/close! ch1)
      (a/close! ch2)
      (is (= [[1 ch1] [2 ch2] [2 ch1]] (<t!! wait  (a/into [] out)))))))
