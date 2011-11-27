(ns clomp.streams-test
  (:use clojure.test
        [clojure.java.io :only [reader writer]])
  (:require [clomp.core :as clomp])
  (:import [java.net Socket]))

(deftest outstream
  (with-open [s   (java.net.Socket. "localhost" 61613)
              out (writer (clomp/outstream s {:destination "/queue/a"}))]
    (clomp/with-connection s {:login "foo" :password "secret"}
      (clomp/subscribe s {:destination "/queue/a"})
      (binding [*out* out]
        (print "cmb")
        (flush))
      (let [received (clomp/receive s)]
        (is (= :MESSAGE (:type received)))
        (is (= "/queue/a" (get-in received [:headers :destination])))
        (is (= "cmb" (:body received)))))))

(deftest outstream-eof
  (with-open [s (java.net.Socket. "localhost" 61613)]
    (clomp/with-connection s {:login "foo" :password "secret"}
      (with-open [out (clomp/writer s {:destination "/queue/a"})]
        (clomp/subscribe s {:destination "/queue/a"})
        (binding [*out* out]
          (print "cmb")
          (flush)))
      (is (= "cmb" (:body (clomp/receive s))))
      (is (get-in (clomp/receive s) [:headers :eof])))))

(deftest instream
  (with-open [s  (java.net.Socket. "localhost" 61613)
              in (reader (clomp/instream s))]
    (clomp/with-connection s {:login "foo" :password "secret"}
      (clomp/subscribe s {:destination "/queue/a"})
      (clomp/send s {:destination "/queue/a"} "hrm\n")
      (clomp/send s {:destination "/queue/a"} "jlb\n")
      (clomp/send s {:destination "/queue/a"} "cmb\n")
      (binding [*in* in]
        (is (= "hrm" (read-line)))
        (is (= "jlb" (read-line)))
        (is (= "cmb" (read-line)))))))

(deftest instream-eof
  (with-open [s (java.net.Socket. "localhost" 61613)
              in (clomp/reader s)]
    (clomp/with-connection s {:login "foo" :password "secret"}
      (clomp/subscribe s {:destination "/queue/a"})
      (clomp/send s {:destination "/queue/a"} "count to thirteen")
      (clomp/send s {:destination "/queue/a" :eof true} "")
      (binding [*in* in]
        (is (= "count to thirteen" (read-line)))
        (is (= nil (read-line)))))))

(deftest instream-outstream
  (with-open [s (java.net.Socket. "localhost" 61613)
              out (clomp/writer s {:destination "/queue/a"})
              in (clomp/reader s)]
    (clomp/with-connection s {:login "foo" :password "secret"}
      (clomp/subscribe s {:destination "/queue/a"})
      (binding [*out* out]
        (println "foo")
        (println "bar")
        (println "baz")
        (flush))
      (binding [*in* in]
        (is (= "foo" (read-line)))
        (is (= "bar" (read-line)))
        (is (= "baz" (read-line)))))))
