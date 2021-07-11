(ns sbocq.cronit
  (:refer-clojure :exclude [next])
  (:require [clojure.set])
  (:import [java.time Duration LocalDateTime ZonedDateTime ZoneId]
           [java.time.temporal ChronoField IsoFields Temporal TemporalField WeekFields]
           [java.time.format DateTimeFormatter]
           [java.util Locale]))


;;;
;;; Misc. utils
;;;

#_(use 'debux.core)

(set! *warn-on-reflection* true)

(defn- bad!
  "Standard error message reporting"
  [msg m] (throw (ex-info msg m)))


;;;
;;; java.time helpers
;;;

(defn- field-adjust
  "C.f. `TemporalField#adjustInto`"
  [^TemporalField tempo-field ^long new-value ^Temporal t]
  (.adjustInto tempo-field t new-value))

(defn- field-from
  "C.f. `TemporalField#getFrom`"
  [^TemporalField tempo-field ^Temporal t]
  (.getFrom tempo-field t))

(defn- locale-for
  "Return a Local object whether locale is provided already as a Locale or as a language
  tag. Defaults to the system locale if it can't be resolved."
  [locale]
  (condp instance? locale
    String (Locale/forLanguageTag locale)
    Locale locale
    (Locale/getDefault)))

(defmulti week-fields-for
  "Return an instance of a `java.time.temporal.WeekFields` from its argument."
  identity)

(defmethod week-fields-for :iso [_] WeekFields/ISO)
(defmethod week-fields-for :sunday-start [_] WeekFields/SUNDAY_START)
(defmethod week-fields-for :default [week-fields-or-locale]
  (if (instance? WeekFields week-fields-or-locale)
    week-fields-or-locale
    (WeekFields/of (locale-for week-fields-or-locale))))

(comment
  (str (week-fields-for :iso))
  #_"WeekFields[MONDAY,4]"
  (str (week-fields-for WeekFields/ISO))
  #_"WeekFields[MONDAY,4]"
  (str (week-fields-for "fr-be"))
  #_"WeekFields[MONDAY,4]"
  (str (week-fields-for "en-us"))
  #_"WeekFields[SUNDAY,1]"
  (str (week-fields-for :sunday-start))
  #_"WeekFields[SUNDAY,1]"
  )

(defn- tempos
  "Mapping between field keywords, their `java.time` counterparts, and their upper ranges."
  ([week-fields]
   (let [^WeekFields week-fields (week-fields-for week-fields)]
     {:second
      {:field ChronoField/SECOND_OF_MINUTE
       :upper-ranges [:minute]}
      :minute
      {:field ChronoField/MINUTE_OF_HOUR
       :upper-ranges [:hour]}
      :hour
      {:field ChronoField/HOUR_OF_DAY
       ;; order of upper ranges is meaningful to identify simplest boards
       :upper-ranges [:day-of-year :day-of-quarter :day-of-month :day-of-week]}
      :day-of-week
      {:field (.dayOfWeek week-fields)
       :upper-ranges [:week-of-year :week-of-month]}
      :day-of-month
      {:field ChronoField/DAY_OF_MONTH
       :upper-ranges [:month]}
      :day-of-quarter
      {:field IsoFields/DAY_OF_QUARTER
       :upper-ranges [:quarter-of-year]}
      :day-of-year
      {:field ChronoField/DAY_OF_YEAR}
      :week-of-month
      {:field (.weekOfMonth week-fields)
       :upper-ranges [:month]}
      :week-of-year
      {:field (.weekOfYear week-fields)}
      :month
      {:field ChronoField/MONTH_OF_YEAR}
      :quarter-of-year
      {:field IsoFields/QUARTER_OF_YEAR}}))
  ([]
   (tempos nil)))

(let [tempos (memoize tempos)]
  (defn- tempo-field
    "Return a java.time.TemporalField from a field keyword."
    (^TemporalField [week-fields field-key]
     (-> (tempos week-fields) field-key :field))
    (^TemporalField [field-key]
     (tempo-field nil field-key))))

(comment
  ;; The basic idea of the algo is an adder with carry over. For example, assuming a naive
  ;; version that iterate only over the next `:day-of-month` and `:month`, the pseudo code
  ;; would look like this (assume a `ValueRange` can be converted into a list using
  ;; `range`):
  (defn next [day-range month-range year-range zdt]
    (if-let [day (first day-range)]
      ;; Day range not empty => there is a next day
      [(rest day-range) month-range year-range
       (field-adjust (tempo-field :day-of-month) day zdt)]
      ;; Day range empty => carry over to month and reset day range to first day of month
      (if-let [month (first month-range)]
        ;; Month range not empty => reset day range on next month and recurse
        (let [zdt (field-adjust (tempo-field :month) month zdt)]
          (recur (range (.rangeRefinedBy (tempo-field :day-of-week) zdt))
                 (rest month-range)
                 year-range
                 zdt))
        ;; Month range empty => carry over to year, reset month range and recurse.
        (let [zdt (field-adjust (tempo-field :day-of-month) (first year-range) zdt)]
          (recur nil
                 (range (.rangeRefinedBy (tempo-field :month) zdt))
                 (rest year-range)
                 zdt)))))
  )

(comment
  ;;;; The case of week fields.
  ;;;; ========================

  ;;;;;; Problem 1. To have such algo working, we must ensure that resetting to the first
  ;; value of a range never rewinds time to the previous month, otherwise we'd return
  ;; multiple times the same time point. Unfortunately, when adjusting the day of a week
  ;; on `:day-of-week` boundaries, `java.time` may change the current week and month.

  ;; Example 1. week 1 of Jul => week 5 of Jun, when reset to the 1st day of its range
  (->> (LocalDateTime/parse "2021-07-01T12:45")
       (.rangeRefinedBy (tempo-field :iso :day-of-week))
       str)
  #_"1 - 7"
  (->> (LocalDateTime/parse "2021-07-01T12:45")
       (field-from (tempo-field :iso :week-of-month)))
  #_1
  (->> (LocalDateTime/parse "2021-07-01T12:45")
       (field-adjust (tempo-field :iso :day-of-week) 1)
       (field-from (tempo-field :iso :week-of-month)))
  #_5  ;If range would have been "4 - 7", we'd reset to the 4th of July and stay in Jul

  ;; Example 2. week 0 of Aug => week 5 of Jul, when reset to the 1st day of its range
  (->> (LocalDateTime/parse "2021-08-01T12:45")
       (.rangeRefinedBy (tempo-field :iso :day-of-week))
       str)
  #_"1 - 7"
  (->> (LocalDateTime/parse "2021-08-01T12:45")
       (field-from (tempo-field :iso :week-of-month)))
  #_0
  (->> (LocalDateTime/parse "2021-08-01T12:45")
       (field-adjust (tempo-field :iso :day-of-week) 1)
       (field-from (tempo-field :iso :week-of-month)))
  #_5  ;If range would have been "7 - 7", we'd reset to the 7th of Aug and stay in Aug

  ;; Solution: We are going to use a specific `day-of-week-bounds` function instead of
  ;; `rangeRefinedBy` to compute the day bounds of a week such that it preserves the
  ;; current current week of the month, and hence the current month.

  ;;;;;; Problem 2. Similarly to problem 1., when adjusting the week of a month or
  ;; year on `:week-of-month` `:week-of-month` or boundaries, `java.time` may change the
  ;; current month. Examples:
  (->> (LocalDateTime/parse "2021-07-05T12:45")
       (.rangeRefinedBy (tempo-field :iso :week-of-month))
       str)
  #_"1 - 5"
  (->> (LocalDateTime/parse "2021-07-05T12:45")
       (field-adjust (tempo-field :iso :week-of-month) 1)
       str)
  #_"2021-06-28T12:45"
  (->> (LocalDateTime/parse "2021-07-05T12:45")
       (field-adjust (tempo-field :iso :week-of-month) 1)
       (field-from (tempo-field :iso :week-of-month)))
  #_5                         ;The first week of the month became week 5 of prev month
  (->> (LocalDateTime/parse "2021-07-11T12:45")
       (field-adjust (tempo-field :iso :week-of-month) 5)
       str)
  #_"2021-08-01T12:45" ; we went to August
  (->> (LocalDateTime/parse "2021-07-11T12:45")
       (field-adjust (tempo-field :iso :week-of-month) 5)
       (field-from (tempo-field :iso :week-of-month)))
  #_0                         ;The last week of the month became week 0 of next month

  ;; Solution: We are going to use a specific `week-adjust` function instead of
  ;; `field-adjust` to adjust weeks on a day that preserves the current month or year.


  ;;;;;; Problem 3. The integer value of a day of week depends on the locale. An
  ;; expression such as `{:day-of-week 1}` would mean Monday using `ISO` week fields, or
  ;; Sunday using `SUNDAY START` week field.

  ;; Take sunday 7th March 2021 for example:
  (field-from (tempo-field :sunday-start :day-of-week)
              (LocalDateTime/parse "2021-03-07T00:00"))
  #_1
  (field-from (tempo-field :iso :day-of-week)
              (LocalDateTime/parse "2021-03-07T00:00"))
  #_7

  ;; Solution: We will provide keywords `:mon` ... `:sun` for the days of week, which will
  ;; be translated to numbers according the locale.


  ;;;;;;; Problem 4. The last issue is that one cannot specify reliably the first or last
  ;; Thursday of the month using the week of month approach because a week may start at `0`
  ;; or `1` depending on the current locale, see (WeekFields
  ;; fields)[https://docs.oracle.com/javase/8/docs/api/java/time/temporal/WeekFields.html].

  ;; Solution: We will make it possible to combine `:day-of-week`, `:day-of-month` and
  ;; negative offsets. For example, the expression below will define all first and last
  ;; Wednesday and Thursdays of every month.
  {:day-of-week [:+ :wed :sun],
   :day-of-month [:+ [:* 1 7] [:* -7 -1]]}
  ;; The implementation will use `{:day-of-week [:+ :wed :sun]}` as a `mask` to "AND" the
  ;; results returned by `{:day-of-month [:+ [:* 1 7] [:* -7 -1]]}`, which are the first
  ;; `7` days and the last `7` days of every month. If a time point does not satifies the
  ;; `mask`, the algo will continue to iterate until it finds one that satifies it.
  )

(defn- day-of-week-bounds
  "Return the min and max days of the current week in the current week of month or week of year depending on week-field-key. Return always a full week if week-field-key is nil."
  [week-fields week-field-key t]
  (if week-field-key
    (let [week-field (tempo-field week-fields week-field-key)
          day-of-week-field (tempo-field week-fields :day-of-week)
          week-range (.rangeRefinedBy week-field t)]
      (condp = (field-from week-field t)
        (.getMinimum week-range)
        {:min (->> (field-adjust ChronoField/DAY_OF_MONTH 1 t)
                   (field-from day-of-week-field))
         :max 7}
        (.getMaximum week-range)
        {:min 1
         :max (->> (field-adjust ChronoField/DAY_OF_MONTH
                                 (.getMaximum (.rangeRefinedBy ChronoField/DAY_OF_MONTH t))
                                 t)
                   (field-from day-of-week-field))}
        {:min 1
         :max 7}))
    {:min 1
     :max 7}))

(defn- week-adjust
  "Adjust the week of a month or of a year such that the month or year field is not
  modified"
  [week-fields week-field-key value t]
  (let [^TemporalField week-field (tempo-field week-fields week-field-key)
        week-range (.rangeRefinedBy week-field t)
        ^TemporalField day-of-week-field (tempo-field week-fields :day-of-week)]
    (condp = value
      (.getMinimum week-range)
      (->> (field-adjust week-field value t)
           (field-adjust day-of-week-field 7))
      (.getMaximum week-range)
      (->> (field-adjust week-field value t)
           (field-adjust day-of-week-field 1))
      (->> (field-adjust week-field value t)
           (field-adjust day-of-week-field (if (> value (field-from week-field t)) 1 7))))))

(comment
  ;; Non overlapping day bounds in a week that overlaps 2 consecutive months, :iso
  (->> (LocalDateTime/parse "2021-07-01T12:45")
       (day-of-week-bounds :iso :week-of-month))
  #_{:min 4, :max 7}                    ;[:thu :fri :sat :sun]
  (->> (LocalDateTime/parse "2021-07-01T12:45")
       (field-adjust (tempo-field :iso :day-of-week) 1)
       str)
  #_"2021-06-28T12:45"
  (->> (LocalDateTime/parse "2021-07-01T12:45")
       (field-adjust (tempo-field :iso :day-of-week) 1)
       (day-of-week-bounds :iso :week-of-month))
  #_{:min 1, :max 3}                    ;[:mon :tue :wed]

  ;; Non overlapping day bounds in a week that overlaps 2 consecutive months, :sunday-start
  (->> (LocalDateTime/parse "2021-07-01T12:45")
       (day-of-week-bounds :sunday-start :week-of-month))
  #_{:min 5, :max 7}                    ;[:thu :fri :sat]
  (->> (LocalDateTime/parse "2021-07-01T12:45")
       (field-adjust (tempo-field :sunday-start :day-of-week) 1)
       str)
  #_"2021-06-27T12:45"
  (->> (LocalDateTime/parse "2021-07-01T12:45")
       (field-adjust (tempo-field :sunday-start :day-of-week) 1)
       (day-of-week-bounds :sunday-start :week-of-month))
  #_{:min 1, :max 4}                    ;[:sun :mon :tue :wed]

  ;; Another example, :iso
  (->> (LocalDateTime/parse "2021-08-01T12:45")
       (day-of-week-bounds :iso :week-of-month))
  #_{:min 7, :max 7}                    ;[:sun]
  (->> (LocalDateTime/parse "2021-08-01T12:45")
       (field-adjust (tempo-field :iso :day-of-week) 1)
       (day-of-week-bounds :iso :week-of-month))
  #_{:min 1, :max 6}                    ;[:mon :tue :wed :thu :fri :sat]

  ;; Another example, :sunday-start
  (->> (LocalDateTime/parse "2021-08-01T12:45")
       (day-of-week-bounds :sunday-start :week-of-month))
  #_{:min 1, :max 7}                    ;[:sun :mon :tue :wed :thu :fri :sat]
  (->> (LocalDateTime/parse "2021-08-01T12:45")
       (field-adjust (tempo-field :sunday-start :day-of-week) 1)
       (day-of-week-bounds :sunday-start :week-of-month))
  #_{:min 1, :max 7}                    ;same as above

  ;; Full week in middle of the month, :iso
  (->> (LocalDateTime/parse "2021-07-15T12:45")
       (day-of-week-bounds :iso :week-of-month))
  #_{:min 1, :max 7}

  ;; Full week in middle of the month, :sunday-start
  (->> (LocalDateTime/parse "2021-07-15T12:45")
       (day-of-week-bounds :iso :week-of-month))
  #_{:min 1, :max 7}

  ;; Partial week in January, :iso
  (->> (LocalDateTime/parse "2021-01-01T12:45")
       (day-of-week-bounds :iso :week-of-month))
  #_{:min 5, :max 7}                    ;[:fri :sat :sun]

  ;; Partial week in January, :sunday-start
  (->> (LocalDateTime/parse "2021-01-01T12:45")
       (day-of-week-bounds :sunday-start :week-of-month))
  #_{:min 6, :max 7}                    ;[:fri :sat]

  ;; Adjusting the week of a month preserves the current month, :iso
  (->> (LocalDateTime/parse "2021-07-05T12:45")
       (.rangeRefinedBy (tempo-field :iso :week-of-month))
       str)
  #_"1 - 5"
  (->> (LocalDateTime/parse "2021-07-05T12:45")
       (week-adjust :iso :week-of-month 1)
       str)
  #_"2021-07-04T12:45"
  (->> (LocalDateTime/parse "2021-07-11T12:45")
       (week-adjust :iso :week-of-month 5)
       str)
  #_"2021-07-26T12:45"
  (->> (LocalDateTime/parse "2021-07-30T12:45:30")
       (week-adjust :iso :week-of-month 1)
       str)
  #_"2021-07-04T12:45:30"

  ;; Adjusting the week remains in same month, :sunday-start
  (->> (LocalDateTime/parse "2021-07-05T12:45")
       (.rangeRefinedBy (tempo-field :sunday-start :week-of-month))
       str)
  #_"1 - 5"
  (->> (LocalDateTime/parse "2021-07-05T12:45")
       (week-adjust :sunday-start :week-of-month 1)
       str)
  #_"2021-07-03T12:45"
  (->> (LocalDateTime/parse "2021-07-11T12:45")
       (week-adjust :sunday-start :week-of-month 5)
       str)
  #_"2021-07-25T12:45"
  (->> (LocalDateTime/parse "2021-07-30T12:45:30")
       (week-adjust :sunday-start :week-of-month 1)
       str)
  #_"2021-07-03T12:45:30"

  ;; day bounds in week of a year care only about months boundaries
  (->> (LocalDateTime/parse "2021-04-26T02:00")
       (day-of-week-bounds :iso :week-of-month))
  #_{:min 1, :max 5}
  (->> (LocalDateTime/parse "2021-04-26T02:00")
       (day-of-week-bounds :iso :week-of-year))
  #_{:min 1, :max 7}
  (->> (LocalDateTime/parse "2021-01-01T02:00")
       (day-of-week-bounds :iso :week-of-year))
  #_{:min 5, :max 7}
  (->> (LocalDateTime/parse "2021-01-01T02:00")
       (day-of-week-bounds :iso :week-of-month))
  #_{:min 5, :max 7}

  ;; Addjusting week of year
  (->> (LocalDateTime/parse "2021-04-25T02:00")
       (field-from (tempo-field :iso :week-of-year)))
  #_16
  (->> (LocalDateTime/parse "2021-04-25T02:00")
       (week-adjust (week-fields-for :iso) :week-of-year 17)
       str)
  #_"2021-04-26T02:00"
  (->> (LocalDateTime/parse "2021-05-09T02:00")
       (week-adjust (week-fields-for :iso) :week-of-year 17)
       str)
  #_"2021-05-02T02:00"

  ;; Adjusting the week of a year preserves the current year.
  (->> (LocalDateTime/parse "2022-04-24T00:00")
       (.rangeRefinedBy (tempo-field (week-fields-for :sunday-start) :week-of-year))
       str)
  #_"1 - 53"
  (->> (LocalDateTime/parse "2022-04-24T00:00")
       (week-adjust (week-fields-for :sunday-start) :week-of-year 1)
       str)
  #_"2022-01-01T00:00" ;must be sunday, not saturday
  (->> (LocalDateTime/parse "2022-04-24T00:00")
       (week-adjust (week-fields-for :sunday-start) :week-of-year 1)
       (field-from (tempo-field :sunday-start :week-of-year)))
  #_1
  (->> (LocalDateTime/parse "2022-04-24T00:00")
       (week-adjust (week-fields-for :sunday-start) :week-of-year 1)
       (day-of-week-bounds :sunday-start :week-of-year))
  #_{:min 7, :max 7}
  )

(defn- indexed-map [vs] (into {} (map-indexed (fn [i v] [v (inc i)]) vs)))

(def day-of-week
  (memoize
   (fn [week-fields]
     (indexed-map (->> (cycle [:mon :tue :wed :thu :fri :sat :sun])
                       (drop (dec (-> ^WeekFields (week-fields-for week-fields)
                                      .getFirstDayOfWeek
                                      .getValue)))
                       (take 7))))))

(def month-of-year (indexed-map [:jan :feb :mar :apr :may :jun :jul :aug
                                 :sep :oct :nov :dec]))

(comment
  (.getValue (.getFirstDayOfWeek (week-fields-for :iso)))
  #_1
  (.getValue (.getFirstDayOfWeek (week-fields-for :sunday-start)))
  #_7

  (:sun (day-of-week :sunday-start))
  #_1

  (:sun (day-of-week :iso))
  #_7
  )


(comment
  ;;;; The case of DST.
  ;;;; ================

  ;;;;;; Problem 1. Hours that overlap when DST stops.
  ;; The problem with overlap is that `java.time` is preserving the original offset
  ;; depending on if we travel forward or backward, while we want to return the same time
  ;; points whether we iterate forward or backward.

  (->> (ZonedDateTime/of 2022 04 03 0 40 0 0 (ZoneId/of "Australia/Lord_Howe"))
       (field-adjust (tempo-field :hour) 1)
       str)
  #_"2022-04-03T01:40+11:00[Australia/Lord_Howe]"
  (->> (ZonedDateTime/of 2022 04 03 2 40 0 0 (ZoneId/of "Australia/Lord_Howe"))
       (field-adjust (tempo-field :hour) 1)
       str)
  #_"2022-04-03T01:40+10:30[Australia/Lord_Howe]"
  (= (->> (ZonedDateTime/of 2022 04 03 0 40 0 0 (ZoneId/of "Australia/Lord_Howe"))
          (field-adjust (tempo-field :hour) 1))
     (->> (ZonedDateTime/of 2022 04 03 2 40 0 0 (ZoneId/of "Australia/Lord_Howe"))
          (field-adjust (tempo-field :hour) 1)))
  #_false

  ;; If not given any context, java.time will default on the earlier offset.
  (-> (ZonedDateTime/of 2022 04 03 1 40 0 0 (ZoneId/of "Australia/Lord_Howe"))
      str)
  #_"2022-04-03T01:40+11:00[Australia/Lord_Howe]"

  ;; Solution: We can do the same using `.withEarlierOffsetAtOverlap` or by adjusting in
  ;; `LocalDateTime.`
  (= (-> (->> (ZonedDateTime/of 2022 04 03 0 40 0 0 (ZoneId/of "Australia/Lord_Howe"))
              .toLocalDateTime
              (field-adjust (tempo-field :hour) 1))
         (ZonedDateTime/of (ZoneId/of "Australia/Lord_Howe")))
     (->> (ZonedDateTime/of 2022 04 03 0 40 0 0 (ZoneId/of "Australia/Lord_Howe"))
          (field-adjust (tempo-field :hour) 1)
          .withEarlierOffsetAtOverlap)
     (-> (->> (ZonedDateTime/of 2022 04 03 2 40 0 0 (ZoneId/of "Australia/Lord_Howe"))
              .toLocalDateTime
              (field-adjust (tempo-field :hour) 1))
         (ZonedDateTime/of (ZoneId/of "Australia/Lord_Howe")))
     (->> (ZonedDateTime/of 2022 04 03 2 40 0 0 (ZoneId/of "Australia/Lord_Howe"))
          (field-adjust (tempo-field :hour) 1)
          .withEarlierOffsetAtOverlap))
  #_true

  ;;;;;; Problem 2. Hours that do not exist when DST starts (gap).
  ;; When adjusting in a gap, `java.time` is automatically correcting the date and the
  ;; result depends on the order in which adjustments are applied.

  (->> (ZonedDateTime/of 2021 10 03 2 10 0 0 (ZoneId/of "Australia/Lord_Howe"))
       str)
  #_"2021-10-03T02:40+11:00[Australia/Lord_Howe]"
  (->> (ZonedDateTime/of 2021 10 03 1 10 0 0 (ZoneId/of "Australia/Lord_Howe"))
       (field-adjust (tempo-field :hour) 2)
       (field-adjust (tempo-field :day-of-month) 2)
       str)
  #_"2021-10-02T02:40+10:30[Australia/Lord_Howe]"
  (->> (ZonedDateTime/of 2021 10 03 1 10 0 0 (ZoneId/of "Australia/Lord_Howe"))
       (field-adjust (tempo-field :day-of-month) 2)
       (field-adjust (tempo-field :hour) 2)
       str)
  #_"2021-10-02T02:10+10:30[Australia/Lord_Howe]"

  ;; => we can be more predictable if we switch to `LocalDateTime` first.
  (-> (->> (ZonedDateTime/of 2021 10 03 1 10 0 0 (ZoneId/of "Australia/Lord_Howe"))
           .toLocalDateTime
           (field-adjust (tempo-field :hour) 2)
           (field-adjust (tempo-field :day-of-month) 2))
      (ZonedDateTime/of (ZoneId/of "Australia/Lord_Howe"))
      str)
  #_"2021-10-02T02:10+10:30[Australia/Lord_Howe]"
  (-> (->> (ZonedDateTime/of 2021 10 03 1 10 0 0 (ZoneId/of "Australia/Lord_Howe"))
           .toLocalDateTime
           (field-adjust (tempo-field :day-of-month) 2)
           (field-adjust (tempo-field :hour) 2))
      (ZonedDateTime/of (ZoneId/of "Australia/Lord_Howe"))
      str)
  #_"2021-10-02T02:10+10:30[Australia/Lord_Howe]"

  ;; And yet, `java.time` automatically *adds* the offset when attempting to set time in a
  ;; gap, which may cause to skip some valid time points. Instead, the correct behavior
  ;; for this library would be to adjust to the end of the gap.
  (-> (->> (ZonedDateTime/of 2021 10 02 1 10 0 0 (ZoneId/of "Australia/Lord_Howe"))
           .toLocalDateTime
           (field-adjust (tempo-field :hour) 2)
           (field-adjust (tempo-field :day-of-month) 3))
      (ZonedDateTime/of (ZoneId/of "Australia/Lord_Howe"))
      str)
  #_"2021-10-03T02:40+11:00[Australia/Lord_Howe]"
  (-> (->> (ZonedDateTime/of 2021 10 02 1 10 0 0 (ZoneId/of "Australia/Lord_Howe"))
           .toLocalDateTime
           (field-adjust (tempo-field :day-of-month) 3)
           (field-adjust (tempo-field :hour) 2))
      (ZonedDateTime/of (ZoneId/of "Australia/Lord_Howe"))
      str)
  #_"2021-10-03T02:40+11:00[Australia/Lord_Howe]"

  ;; Solution: First, we will always adjust field in `LocalDateTime.` When adding a zone
  ;; to the `LocalDateTime`, we will return the time point unless it falls in a gap. If
  ;; it does, we will return the first time point after the gap even though it is
  ;; unaligned (gap condition) to simulate the standard behavior of cron.
  )

(defn- zone-ldt
  "Convert a `LocalDateTime` to a `ZonedDateTime.` If the `LocalDateTime` is in a gap,
  return earliest and latest `LocalDateTimes` around the gap"
  [^ZoneId zone-id ^LocalDateTime ldt]
  (let [trans (-> (.getRules zone-id)
                  (.getTransition ldt))]
    (if (and trans (.isGap trans))
       ;; 'before' is actually misleading because it is in the gap.
       ;; We correct this by substracting 1 second
      {:before-gap (.minus (.getDateTimeBefore trans) (Duration/ofMillis 1000))
       :after-gap (.getDateTimeAfter trans)}
      {:zdt (ZonedDateTime/of ldt zone-id)})))

(comment
  ;; Gap. [2:00:00am,2:29:59am] does not exist on +11
  (->> (ZonedDateTime/of 2021 10 02 1 10 0 0 (ZoneId/of "Australia/Lord_Howe"))
       .toLocalDateTime
       (field-adjust (tempo-field :day-of-month) 3)
       (field-adjust (tempo-field :hour) 2)
       (zone-ldt (ZoneId/of "Australia/Lord_Howe")))
  #_{:before-gap "2021-10-03T01:59:59", :after-gap "2021-10-03T02:30"}

  ;; Normal
  (->> (ZonedDateTime/of 2021 10 02 1 10 0 0 (ZoneId/of "Australia/Lord_Howe"))
       .toLocalDateTime
       (field-adjust (tempo-field :day-of-month) 4)
       (field-adjust (tempo-field :hour) 2)
       (zone-ldt (ZoneId/of "Australia/Lord_Howe")))
  #_{:zdt "2021-10-04T02:10+11:00[Australia/Lord_Howe]"}

  (->> (LocalDateTime/parse "2021-03-28T02:00")
       (zone-ldt (ZoneId/of "Europe/Brussels")))
  #_{:before-gap "2021-03-28T01:59:59", :after-gap  "2021-03-28T03:00"}

  ;; Overlap => earlier offset
  (= (-> (->> (ZonedDateTime/of 2022 04 03 0 40 0 0 (ZoneId/of "Australia/Lord_Howe"))
              .toLocalDateTime
              (field-adjust (tempo-field :hour) 1))
         (ZonedDateTime/of (ZoneId/of "Australia/Lord_Howe")))
     (->> (ZonedDateTime/of 2022 04 03 0 40 0 0 (ZoneId/of "Australia/Lord_Howe"))
          (field-adjust (tempo-field :hour) 1)
          .withEarlierOffsetAtOverlap)
     (->> (ZonedDateTime/of 2022 04 03 0 40 0 0 (ZoneId/of "Australia/Lord_Howe"))
          .toLocalDateTime
          (field-adjust (tempo-field :hour) 1)
          (zone-ldt (ZoneId/of "Australia/Lord_Howe"))
          :zdt)
     (-> (->> (ZonedDateTime/of 2022 04 03 2 40 0 0 (ZoneId/of "Australia/Lord_Howe"))
              .toLocalDateTime
              (field-adjust (tempo-field :hour) 1))
         (ZonedDateTime/of (ZoneId/of "Australia/Lord_Howe")))
     (->> (ZonedDateTime/of 2022 04 03 2 40 0 0 (ZoneId/of "Australia/Lord_Howe"))
          (field-adjust (tempo-field :hour) 1)
          .withEarlierOffsetAtOverlap)
     (->> (ZonedDateTime/of 2022 04 03 2 40 0 0 (ZoneId/of "Australia/Lord_Howe"))
          .toLocalDateTime
          (field-adjust (tempo-field :hour) 1)
          (zone-ldt (ZoneId/of "Australia/Lord_Howe"))
          :zdt))
  #_true
  )

;; Wrapping java-time

(defn tempo-get
  "Mapping from field keys to objects and values frequently used by this library."
  [week-fields week-field-key field-key]
  (let [^TemporalField tempo-field (if (= field-key :year)
                                     ChronoField/YEAR
                                     (tempo-field week-fields field-key))
        range (.range tempo-field)
        range-fixed? (.isFixed range)
        range-min (.getMinimum range)
        range-max (.getMaximum range)
        fixed-bounds {:min range-min, :max range-max}]
    {:key field-key
     :field tempo-field
     :bounds-fn (cond (= field-key :day-of-week)
                      (fn [t]
                        (day-of-week-bounds week-fields week-field-key t))
                      range-fixed? (constantly fixed-bounds)
                      :else (fn [t]
                              (let [range (.rangeRefinedBy tempo-field t)]
                                {:min (.getMinimum range)
                                 :max (.getMaximum range)})))
     :adjust-fn (case field-key
                  (:week-of-month :week-of-year)
                  (fn [v t] (week-adjust week-fields field-key v t))
                  (fn [v t] (field-adjust tempo-field v t)))
     :from-fn (fn [t] (field-from tempo-field t))
     :min range-min
     :max range-max}))


;;;
;;; Expressions
;;;

(defn- read-field-expr
  "Allow for syntactic sugar for expressions, see rich comment below for examples."
  [week-fields field-key field-expr]
  (let [tagged-vec
        (cond (coll? field-expr)
              (let [[head & tail] field-expr]
                (cond (number? head) (into [:+] field-expr)
                      (= :* head) (cond (= 2 (count tail)) (conj field-expr 1)
                                        :else field-expr)
                      :else (into [:+] (map (fn [v] (if (coll? v)
                                                      (read-field-expr week-fields
                                                                       field-key
                                                                       v)
                                                      v))
                                            (rest field-expr)))))
              (= :* field-expr) [:*]
              (= :+ field-expr) [:+]
              :else [:+ field-expr])]
    (into [(first tagged-vec)]
          (map (letfn [(to-num [m] (fn [v] (or (m v) v)))]
                 (case field-key
                   :day-of-week (to-num (day-of-week week-fields))
                   :month-of-year (to-num month-of-year)
                   identity))
               (rest tagged-vec)))))

(comment
  ;; range expressions
  (read-field-expr nil :_ :*) #_[:*]
  (read-field-expr nil :_ [:*]) #_[:*]
  (read-field-expr nil :_ [:* 2]) #_[:* 2]
  (read-field-expr nil :_ [:* 10 20]) #_[:* 10 20 1]
  (read-field-expr nil :_ [:* 10 20 5]) #_[:* 10 20 5]
  (read-field-expr nil :_ [:* -3 -1 1]) #_[:* -3 -1 1]
  (read-field-expr :iso :day-of-week [:* :mon :fri]) #_[:* 1 5 1]
  (read-field-expr :iso :day-of-week :thu) #_[:+ 4]
  (read-field-expr :sunday-start :day-of-week [:* :mon :fri]) #_[:* 2 6 1]
  (read-field-expr :sunday-start :day-of-week :thu) #_[:+ 5]

  ;; offsets expressions
  (read-field-expr nil :_ :+) #_[:+]
  (read-field-expr nil :_ 2) #_[:+ 2]
  (read-field-expr nil :_ [2 5]) #_[:+ 2 5]
  (read-field-expr nil :_ [:+ 2 5]) #_[:+ 2 5]
  (read-field-expr nil :_ [:+ 2 [:* 3 8 2]]) #_[:+ 2 [:* 3 8 2]]
  (read-field-expr nil :month-of-year [:+ :jan :apr [:* :jun :aug]]) #_[:+ 1 4 [:* 6 8 1]]
  (read-field-expr nil :month-of-year
                   [:+ :jan :apr [:* :jun :aug]])
  )

;;;
;;; Parsers and iterators for enum [:+] and range [:*] expressions.
;;;
;;; We start by defining basic range and enum field iterators independently from dates and
;;; timezones to decompose the problem in a first simpler step. In a second step,
;;; clip-min and clip-max will be set to the min and max bounds of temporal fields.
;;;

(defn- to-pos-value
  "Convert a negative value relative to the end of a clip to a positive one."
  [clip-max v]
  (if (<= 0 v) v (recur clip-max (inc (+ clip-max v)))))

(defn- clip-range
  "Clip a range expression on an interval and convert negative bounds to positive ones"
  [clip-min clip-max range-expr]
  (let [[star-kw star-min star-max star-step] (case (count range-expr)
                                                1 [:* nil nil 1]
                                                2 [:* nil nil (second range-expr)]
                                                range-expr)]
    (when-not (= :* star-kw)
      (bad! "range-invalid-field" {:field range-expr}))
    (let [star-min (to-pos-value clip-max (or star-min clip-min))
          star-max (to-pos-value clip-max (or star-max clip-max))
          star-step (or star-step 1)]
      (when (< star-max star-min)
        (bad! "range-step-must-be-positive" {:field range-expr}))
      (if (pos? star-step)
        [(max star-min clip-min) (min clip-max star-max) star-step]
        (bad! "range-step-must-be-positive" {:field range-expr, :step star-step})))))

(comment
  (clip-range 3 8 [:*]) #_[3 8 1]
  (clip-range 3 8 [:* 1]) #_[3 8 1]
  (clip-range 3 8 [:* 2]) #_[3 8 2]
  (clip-range 3 8 [:* 4 10 2]) #_[4 8 2]
  (clip-range 3 8 [:* 2 6]) #_[3 6 1]
  (clip-range 3 8 [:* 1 2]) #_[3 2 1]  ;empty range
  (clip-range 3 8 [:* -3 -1]) #_[6 8 1]
  (clip-range 3 8 [:* -1 -3]) #_error
  (clip-range 3 8 [:* -1 -1]) #_[8 8 1]
  )

(defn- clip-enum
  "Clip an enum expression on an interval and convert negative offsets to positive ones"
  [clip-min clip-max enum-expr]
  (letfn [(normalize-range-to-enum [range-expr]
            (let [[range-min range-max step] (clip-range clip-min clip-max range-expr)]
              (range range-min (inc range-max) step)))
          (normalize-enum-vals [enum-vals]
            (reduce (fn [vs v]
                      (cond (coll? v) (into vs (case (first v)
                                                 :* (normalize-range-to-enum v)
                                                 :+ (normalize-enum-vals (rest v))
                                                 (bad! "invalid-sub-field"
                                                       {:field enum-expr
                                                        :sub-field v})))
                            (number? v) (let [v (to-pos-value clip-max v)]
                                          (if (<= clip-min v clip-max)
                                            (conj vs v)
                                            vs))
                            :else (bad! "invalid-enumeration" {:field enum-expr
                                                               :enum enum-vals})))
                    []
                    (or (seq enum-vals) [clip-min])))]
    (case (first enum-expr)
      :+ (vec (sort (normalize-enum-vals (rest enum-expr))))
      :* (vec (normalize-range-to-enum enum-expr))
      (bad! "invalid-field" {:field enum-expr}))))

(comment
  (clip-enum 3 7 [:+]) #_[3]
  (clip-enum 0 59 [:+ 0]) #_[0]
  (clip-enum 3 8 [:+ 3 6 7]) #_[3 6 7]
  (clip-enum 3 8 [:* 5 10 2]) #_[5 7]
  (clip-enum 3 8 [:+ 1 4 [:* 5 10 2]]) #_[4 5 7]
  (clip-enum 3 8 [:+ 4 -1]) #_[4 8]
  (clip-enum 3 8 [:* 1]) #_[3 4 5 6 7 8]
  (clip-enum 3 8 [:* 4 10 2]) #_[4 6 8]
  (clip-enum 3 8 [:* 2 6]) #_[3 4 5 6]
  (clip-enum 3 8 [:* -3 -1]) #_[6 7 8]
  (clip-enum 3 8 [:+ 9]) #_[]
  )


(defn- containerp
  "Given a field expression, return a function that checks whether a value satisfies this
  field expression for some min and max bounds. Note that negative offsets in field
  expressions are added to the max bound plus one. This will used to filter out time
  points that do not satisfy a field expression applied as a mask."
  [field-expr]
  ;; We can afford to memoize because the ranges of time fields are often identical
  (case (first field-expr)
    :* (let [memo-clip-range (memoize clip-range)]
         (fn [min max v] (let [[min max step] (memo-clip-range min max field-expr)]
                           (and (<= min v max) (= 0 (mod (- v min) step))))))
    :+ (let [memo-clip-enum (memoize clip-enum)]
         (fn [min max v] (let [vs (memo-clip-enum min max field-expr)]
                           (.contains ^clojure.lang.PersistentVector vs v))))))

(comment
  ((containerp [:+ 3 6 7]) 3 8 4) #_false
  ((containerp [:+ 3 6 7]) 3 8 6) #_true
  ((containerp [:+ -1 -2 -3]) 3 8 6) #_true ;[6 7 8 are valid]
  ((containerp [:+ -1 -2 -3]) 3 8 5) #_false
  ((containerp [:* -3 -1 1]) 3 8 6) #_true ;[6 7 8 are valid]
  ((containerp [:* -3 -1 1]) 3 8 5) #_false
  )


;;;; Range and enum value iterators

(defn- ->range-iterator [min prev current next max step]
  (cond-> {:min min, :current current, :max max, :step step}
    (<= min prev) (assoc :prev prev)
    (<= next max) (assoc :next next)))

(defn- range-iterator-init [[min max step] v]
  (if (> min max)
    (-> (->range-iterator min (dec min) v (inc max) max step)
        (assoc :init-aligned? false))
    (let [max (- max (mod (- max min) step))
          [aligned? prev next] (cond (< v min) [false (dec min) min]
                                     (> v max) [false max (inc max)]
                                     :else (let [prev (- v (mod (- v min) step))]
                                             (if (= prev v)
                                               [true (- prev step) (+ prev step)]
                                               [false prev (+ prev step)])))]
      (-> (->range-iterator min prev v next max step)
          (assoc :init-aligned? aligned?)))))

(defn- range-iterator-next [{:keys [:min :next :max :step]}]
  (when next
    (->range-iterator min (- next step) next (+ next step) max step)))

(defn- range-iterator-first [{:keys [:min :max :step]}]
  (when (<= min max)
    (->range-iterator min (- min step) min (+ min step) max step)))

(defn- range-iterator-prev [{:keys [:min :prev :max :step]}]
  (when prev
    (->range-iterator min (- prev step) prev (+ prev step) max step)))

(defn- range-iterator-last [{:keys [:min :max :step]}]
  (when (<= min max)
    (->range-iterator min (- max step) max (+ max step) max step)))

(comment
  (clip-range 3 8 [:* 4 10 2])
  #_[4 8 2]

  (range-iterator-init (clip-range 2 8 [:* 2 4 2]) 2)
  #_{:min 2, :current 2, :max 4, :step 2, :next 4, :init-aligned? true}

  (range-iterator-init (clip-range 3 8 [:* 4 10 2]) 3)
  #_{:min 4, :current 3, :max 8, :step 2, :next 4, :init-aligned? false}

  (range-iterator-init (clip-range 3 8 [:* 4 10 2]) 4)
  #_{:min 4, :current 4, :max 8, :step 2, :next 6, :init-aligned? true}

  (range-iterator-init (clip-range 3 8 [:* 4 10 2]) 5)
  #_{:min 4, :current 5, :max 8, :step 2, :prev 4, :next 6, :init-aligned? false}

  (range-iterator-init (clip-range 3 8 [:* 4 10 2]) 6)
  #_{:min 4, :current 6, :max 8, :step 2, :prev 4, :next 8, :init-aligned? true}

  (range-iterator-init (clip-range 3 8 [:* 4 10 2]) 7)
  #_  {:min 4, :current 7, :max 8, :step 2, :prev 6, :next 8, :init-aligned? false}

  (range-iterator-init (clip-range 3 8 [:* 4 10 2]) 8)
  #_{:min 4, :current 8, :max 8, :step 2, :prev 6, :init-aligned? true}

  (range-iterator-init (clip-range 3 8 [:* 4 10 2]) 10)
  #_{:min 4, :current 10, :max 8, :step 2, :prev 8, :init-aligned? false}

  (range-iterator-init (clip-range 3 8 [:* 10 12 2]) 4)
  #_{:min 10, :current 4, :max 8, :step 2, :init-aligned? false}

  (range-iterator-init [1 12 1] 14)
  #_{:min 1, :current 14, :max 12, :step 1, :prev 12, :init-aligned? false}

  (range-iterator-init [1 6 2] 8)
  #_{:min 1, :current 8, :max 5, :step 2, :prev 5, :init-aligned? false}

  (-> (range-iterator-init (clip-range 3 8 [:* 10 12 2]) 4)
      range-iterator-first)
  #_nil

  (-> (range-iterator-init (clip-range 3 8 [:* 4 10 2]) 3)
      range-iterator-first)
  #_{:min 4, :current 4, :max 8, :step 2, :next 6}

  (-> (range-iterator-init (clip-range 3 8 [:* 4 10 2]) 3)
      range-iterator-last)
  #_{:min 4, :current 8, :max 8, :step 2, :prev 6}

  (-> (range-iterator-init (clip-range 1 10 [:* 4 10 4]) 3)
      range-iterator-last)
  #_{:min 4, :current 8, :max 8, :step 4, :prev 4}

  (-> (range-iterator-init (clip-range 3 8 [:* 4 10 2]) 5)
      (doto prn)
      #_{:min 4, :current 5, :max 8, :step 2, :prev 4, :next 6, :init-aligned? false}
      range-iterator-next
      (doto prn)
      #_{:min 4, :current 6, :max 8, :step 2, :prev 4, :next 8}
      range-iterator-next
      (doto prn)
      #_{:min 4, :current 8, :max 8, :step 2, :prev 6}
      range-iterator-next
      #_nil)
  (-> (range-iterator-init (clip-range 3 8 [:* 4 10 2]) 5)
      (doto prn)
      #_{:min 4, :current 5, :max 8, :step 2, :prev 4, :next 6, :init-aligned? false}
      range-iterator-prev
      (doto prn)
      #_{:min 4, :current 4, :max 8, :step 2, :next 6}
      range-iterator-prev
      #_nil)
  )

(defn- ->enum-iterator [^ints enum-array prev-index current next-index]
  (cond-> {:enum-array enum-array, :current current}
    (<= 0 prev-index) (assoc :prev-index prev-index
                             :prev (aget enum-array prev-index))
    (< next-index (alength enum-array)) (assoc :next-index next-index
                                               :next (aget enum-array next-index))))

(defn- enum-iterator-init [enum-values v]
  (let [len (count enum-values)]
    (if (= 0 len)
      (-> (->enum-iterator (int-array enum-values) -1 v 1)
          (assoc :init-aligned? false))
      (loop [index 0, value (enum-values 0)]
        (let [next-index (inc index)]
          (cond (< v value)
                , (-> (->enum-iterator (int-array enum-values) (dec index) v index)
                      (assoc :init-aligned? false))
                (= v value)
                , (-> (->enum-iterator (int-array enum-values) (dec index) v next-index)
                      (assoc :init-aligned? true))
                (< next-index len)
                , (recur next-index (enum-values next-index))
                :else
                , (-> (->enum-iterator (int-array enum-values) (dec len) v next-index)
                      (assoc :init-aligned? false))))))))

(defn- enum-iterator-next [{:keys [:enum-array :next :next-index]}]
  (when next-index
    (->enum-iterator enum-array (dec next-index) next (inc next-index))))

(defn- enum-iterator-first [{:keys [:enum-array]}]
  (when (> (alength ^ints enum-array) 0)
    (->enum-iterator enum-array -1 (aget ^ints enum-array 0) 1)))

(defn- enum-iterator-prev [{:keys [:enum-array :prev :prev-index]}]
  (when prev-index
    (->enum-iterator enum-array (dec prev-index) prev (inc prev-index))))

(defn- enum-iterator-last [{:keys [:enum-array]}]
  (when-let [last-idx (let [len (alength ^ints enum-array)]
                        (and (< 0 len) (dec len)))]
    (->enum-iterator enum-array
                     (dec last-idx) (aget ^ints enum-array last-idx) (inc last-idx))))

(comment
  (clip-enum 3 8 [:* 4 10 2])
  #_[4 6 8]

  (-> (enum-iterator-init (clip-enum 3 8 [:* 4 10 2]) 3)
      (dissoc :enum-array))
  #_{:current 3, :next-index 0, :next 4}

  (-> (enum-iterator-init (clip-enum 3 8 [:* 4 10 2]) 4)
      (dissoc :enum-array))
  #_{:current 4, :next-index 1, :next 6, :init-aligned? true}

  (-> (enum-iterator-init (clip-enum 3 8 [:* 4 10 2]) 5)
      (dissoc :enum-array))
  #_{:current 5, :prev-index 0, :prev 4, :next-index 1, :next 6}

  (-> (enum-iterator-init (clip-enum 3 8 [:* 4 10 2]) 6)
      (dissoc :enum-array))
  #_{:current 6, :prev-index 0, :prev 4, :next-index 2, :next 8, :init-aligned? true}

  (-> (enum-iterator-init (clip-enum 3 8 [:* 4 10 2]) 7)
      (dissoc :enum-array))
  #_{:current 7, :prev-index 1, :prev 6, :next-index 2, :next 8}

  (-> (enum-iterator-init (clip-enum 3 8 [:* 4 10 2]) 8)
      (dissoc :enum-array))
  #_{:current 8, :prev-index 1, :prev 6, :init-aligned? true}

  (-> (enum-iterator-init (clip-enum 3 8 [:* 4 10 2]) 10)
      (dissoc :enum-array))
  #_{:current 10, :prev-index 2, :prev 8}

  (-> (enum-iterator-init (clip-enum 3 8 [:+ 2]) 3)
      (dissoc :enum-array))
  #_{:current 3, :init-aligned? false}

  (-> (enum-iterator-init (clip-enum 3 8 [:* 4 10 2]) 3)
      enum-iterator-first
      (dissoc :enum-array))
  #_{:current 4, :next-index 1, :next 6}

  (-> (enum-iterator-init (clip-enum 3 8 [:* 4 10 2]) 3)
      enum-iterator-last
      (dissoc :enum-array))
  #_{:current 8, :prev-index 1, :prev 6}

  (-> (enum-iterator-init (clip-enum 1 8 [:+ 2]) 2)
      enum-iterator-last
      (dissoc :enum-array))
  #_{:current 2}

  (-> (enum-iterator-init (clip-enum 3 8 [:+ 2]) 3)
      enum-iterator-first
      (dissoc :enum-array))
  #_nil

  (-> (enum-iterator-init (clip-enum 3 8 [:+ 2]) 3)
      enum-iterator-last
      (dissoc :enum-array))
  #_nil

  (-> (enum-iterator-init (clip-enum 3 8 [:* 4 10 2]) 5)
      (doto (-> (dissoc :enum-array) prn))
      #_{:current 5, :prev-index 0, :prev 4, :next-index 1, :next 6}
      enum-iterator-next
      (doto (-> (dissoc :enum-array) prn))
      #_{:current 6, :prev-index 0, :prev 4, :next-index 2, :next 8}
      enum-iterator-next
      (doto (-> (dissoc :enum-array) prn))
      #_{:current 8, :prev-index 1, :prev 6}
      enum-iterator-next
      #_nil)
  (-> (enum-iterator-init (clip-enum 3 8 [:* 4 10 2]) 5)
      (doto (-> (dissoc :enum-array) prn))
      #_{:current 5, :prev-index 0, :prev 4, :next-index 1, :next 6}
      enum-iterator-prev
      (doto (-> (dissoc :enum-array) prn))
      #_{:current 4, :next-index 1, :next 6}
      range-iterator-prev
      #_nil)
  )


;;;
;;; Wrap enum and range iterators into a generic temporal field iterator and adjuster
;;;

(defprotocol PFieldIterator
  (iterator-reset [_ t]
    "Reset this iterator on the corresponding time field of a temporal")
  (iterator-first [_ t]
    "Return the first iterator with the temporal having its field adjusted accordingly")
  (iterator-next [_  t]
    "Return the next iterator with the temporal having its field adjusted accordingly")
  (iterator-prev [_  t]
    "Return the prev iterator with the temporal having its field adjusted accordingly")
  (iterator-last [_  t]
    "Return the last iterator with the temporal having its field adjusted accordingly")
  (iterator-aligned? [_]
    "True if the current iterator value is aligned (may only be false after a reset)"))

(defrecord RangeFieldIterator [field-expr field-bounds field-adjust field-from,
                               range-iterator]
  PFieldIterator
  (iterator-reset [this t]
    (let [{:keys [min max]} (field-bounds t)
          range-iterator (range-iterator-init (clip-range min max field-expr)
                                              (field-from t))]
      (assoc this :range-iterator range-iterator)))
  (iterator-first [this t]
    (when-let [range-iterator (range-iterator-first range-iterator)]
      [(assoc this :range-iterator range-iterator)
       (field-adjust (:current range-iterator) t)]))
  (iterator-next [this t]
    (when-let [range-iterator (range-iterator-next range-iterator)]
      [(assoc this :range-iterator range-iterator)
       (field-adjust (:current range-iterator) t)]))
  (iterator-prev [this t]
    (when-let [range-iterator (range-iterator-prev range-iterator)]
      [(assoc this :range-iterator range-iterator)
       (field-adjust (:current range-iterator) t)]))
  (iterator-last [this t]
    (when-let [range-iterator (range-iterator-last range-iterator)]
      [(assoc this :range-iterator range-iterator)
       (field-adjust (:current range-iterator) t)]))
  (iterator-aligned? [_]
    (let [init-aligned? (:init-aligned? range-iterator)]
      (or init-aligned? (nil? init-aligned?)))))

(defrecord EnumFieldIterator [field-expr field-bounds field-adjust field-from,
                              enum-iterator]
  PFieldIterator
  (iterator-reset [this t]
    (let [{:keys [min max]} (field-bounds t)
          enum-iterator (enum-iterator-init (clip-enum min max field-expr)
                                            (field-from t))]
      (assoc this :enum-iterator enum-iterator)))
  (iterator-first [this t]
    (when-let [enum-iterator (enum-iterator-first enum-iterator)]
      [(assoc this :enum-iterator enum-iterator)
       (field-adjust (:current enum-iterator) t)]))
  (iterator-next [this t]
    (when-let [enum-iterator (enum-iterator-next enum-iterator)]
      [(assoc this :enum-iterator enum-iterator)
       (field-adjust (:current enum-iterator) t)]))
  (iterator-prev [this t]
    (when-let [enum-iterator (enum-iterator-prev enum-iterator)]
      [(assoc this :enum-iterator enum-iterator)
       (field-adjust (:current enum-iterator) t)]))
  (iterator-last [this t]
    (when-let [enum-iterator (enum-iterator-last enum-iterator)]
      [(assoc this :enum-iterator enum-iterator)
       (field-adjust (:current enum-iterator) t)]))
  (iterator-aligned? [_]
    (let [init-aligned? (:init-aligned? enum-iterator)]
      (or init-aligned? (nil? init-aligned?)))))

(defn- ->field-iterator [field-tempo field-expr t]
  (let [{:keys [:key :bounds-fn :adjust-fn :from-fn]} field-tempo]
    (-> (case (first field-expr)
          :* (->RangeFieldIterator field-expr bounds-fn adjust-fn from-fn nil)
          :+ (->EnumFieldIterator field-expr bounds-fn adjust-fn from-fn nil))
        (iterator-reset t)
        (assoc :field-key key))))       ;for debug


;;;
;;; Wrapping it up
;;;

(def board-fields
  "The supported fields"
  (letfn [(rec [seen field-key]
            (if (some #{field-key} seen)
              seen
              (reduce rec
                      (conj seen field-key)
                      (reverse (-> (tempos) field-key :upper-ranges)))))]
    (vec (rec [] :second))))

(comment
  board-fields
  #_[:second :minute :hour :day-of-week :week-of-month :month :week-of-year
     :day-of-month :day-of-quarter :quarter-of-year :day-of-year]
  )

;; Boards capture valid combination of fields in a cronit expression, from least to most
;; complicated.
;;
(def boards
  "List of all the valid cronit boards, from least to most complicated"
  (letfn [(rec [boards field-key]
            (let [boards (map #(conj % field-key) boards)]
              ;; assumes upper ranges that skip the most fields come first
              (if-let [upper-ranges (-> (tempos) field-key :upper-ranges)]
                (mapcat (partial rec boards) upper-ranges)
                boards)))]
    (vec (rec [[]] :second))))

(comment
  boards
  #_[[:second :minute :hour :day-of-year]
     [:second :minute :hour :day-of-quarter :quarter-of-year]
     [:second :minute :hour :day-of-month :month]
     [:second :minute :hour :day-of-week :week-of-year]
     [:second :minute :hour :day-of-week :week-of-month :month]]
  )

(def c-expr-opts #{:week-fields :locale})

(defn board-for
  "Return the simplest `:board` that contains most fields in a `c-expr`. Return remaining
  fields that do not fit on a board as `:mask`"
  [c-expr]
  (letfn [(board-weight [board]
            (reduce (fn [weight field-key]
                      (+ weight
                         (if-let [field-expr (field-key c-expr)]
                           (let [{:keys [:min :max]} (tempo-get :iso nil field-key)
                                 field-expr (read-field-expr :iso field-key field-expr)]
                             (case (first field-expr)
                               :+ (count (clip-enum min max field-expr))
                               :* (let [[min max step] (clip-range min max
                                                                   field-expr)]
                                    (if (> max min)
                                      (int (/ (- max min) step))
                                      0))))
                           0)))
                    0
                    board))]
    (let [candidates (map (juxt identity #(count (filter c-expr %))) boards)
          max-len (reduce max (map second candidates))
          boards (map first (filter #(= max-len (second %)) candidates))
          board (if (= (count boards) 1)
                  (first boards)
                  (ffirst (->> (map (fn [board] [board (board-weight board)]) boards)
                               (sort-by second))))]
      {:board board
       :mask (vec (keys (apply dissoc c-expr (into board c-expr-opts))))})))

(comment
  (board-for {:minute 1 :day-of-month 2})
  #_{:board [:second :minute :hour :day-of-month :month], :mask []}


  (board-for {:minute 1
              :day-of-week [:+ :sun]
              :day-of-month [:+ [:* 1 14] [:* -7 -1]]})
  #_{:board [:second :minute :hour :day-of-week :week-of-year], :mask [:day-of-month]}

  (board-for {:minute 10, :hour 2, :day-of-week [1 2],
              :week-of-month [:+ [:* 1]]
              :month [:+ 3 5 10]
              :week-fields :iso})
  #_{:board [:second :minute :hour :day-of-week :week-of-month :month], :mask []}
  )

(defn- read-c-expr
  "Read a cronit expression, infers unspecied fields, and return its expansion together
  with the simplest `:board` it fits on. Any time field that does not fit on a board is
  returned as `:mask`. Options are extracted from the cronit expressions and returned
  separately as `:opts`."
  [c-expr]
  (let [board-rem (board-for (dissoc c-expr :year))
        board (conj (:board board-rem) :year)
        mask (:mask board-rem)
        opts (select-keys c-expr c-expr-opts)
        week-fields (week-fields-for (or (:week-fields opts) (:locale opts)))
        c-expr (assoc c-expr :year :*)
        [_ top-recurrence c-expr']
        ;; We traverse a board from left to right, i.e. from smallest time field to
        ;; biggest one. All undefined time fields to the left of the first user defined
        ;; field are inferred as [:+]. For example, {:hour 2} means
        ;; '00-00-02-*-*-*', or {:month 2} means '00-00-00-01-02-*'. The top-recurrence
        ;; is the smallest time field that is [:*] and after which there are only [:*].
        (reduce (fn [[client-defined? top-recurrence m] field-key]
                  (let [field-expr (when-let [field-expr (field-key c-expr)]
                                     (read-field-expr week-fields field-key field-expr))
                        tag (first field-expr)]
                    (if client-defined?
                      (case (or tag :*)
                        :+ [true nil (assoc m field-key field-expr)]
                        :* [true (or top-recurrence field-key) (assoc m
                                                                      field-key
                                                                      (or field-expr
                                                                          [:*]))])
                      (case (or tag :+)
                        :+ [field-expr nil (assoc m field-key (or field-expr [:+]))]
                        :* [field-expr
                            (or top-recurrence field-key)
                            (assoc m field-key field-expr)]))))
                [false nil {}]
                board)]
    {:c-expr (into c-expr'
                   (map (fn [field-key]
                          [field-key
                           (read-field-expr week-fields field-key (field-key c-expr))])
                        mask))
     :board board
     :mask mask
     :opts opts
     :top-recurrence (or top-recurrence :year)}))

(comment
  (read-c-expr {})
  #_{:c-expr {:second [:+], :minute [:+], :hour [:+], :day-of-year [:+], :year [:*]},
     :board [:second :minute :hour :day-of-year :year],
     :mask [],
     :opts {},
     :top-recurrence :year}

  (read-c-expr {:second :*})
  #_{:c-expr {:second [:*], :minute [:*], :hour [:*], :day-of-year [:*], :year [:*]},
     :board [:second :minute :hour :day-of-year :year],
     :mask [],
     :opts {},
     :top-recurrence :second}

  (read-c-expr {:second [:* 30]})
  #_{:c-expr {:second [:* 30], :minute [:*], :hour [:*], :day-of-year [:*], :year [:*]},
     :board [:second :minute :hour :day-of-year :year],
     :mask [],
     :opts {},
     :top-recurrence :second}

  (read-c-expr {:second 5})
  #_{:c-expr {:second [:+ 5], :minute [:*], :hour [:*], :day-of-year [:*], :year [:*]},
     :board [:second :minute :hour :day-of-year :year],
     :mask [],
     :opts {},
     :top-recurrence :minute}

  (read-c-expr {:minute :*})
  #_{:c-expr {:second [:+], :minute [:*], :hour [:*], :day-of-year [:*], :year [:*]},
     :board [:second :minute :hour :day-of-year :year],
     :mask [],
     :opts {},
     :top-recurrence :minute}

  (read-c-expr {:hour 2})
  #_{:c-expr {:second [:+], :minute [:+], :hour [:+ 2], :day-of-year [:*], :year [:*]},
     :board [:second :minute :hour :day-of-year :year],
     :mask [],
     :opts {},
     :top-recurrence :day-of-year}

  (read-c-expr {:second 5 :hour 2})
  #_{:c-expr {:second [:+ 5], :minute [:*], :hour [:+ 2], :day-of-year [:*], :year [:*]},
     :board [:second :minute :hour :day-of-year :year],
     :mask [],
     :opts {},
     :top-recurrence :day-of-year}

  (read-c-expr {:second 5 :hour 2 :day-of-year [:* 2]})
  #_{:c-expr {:second [:+ 5], :minute [:*], :hour [:+ 2], :day-of-year [:* 2], :year [:*]},
     :board [:second :minute :hour :day-of-year :year],
     :mask [],
     :opts {},
     :top-recurrence :day-of-year}

  (read-c-expr {:minute 1 :day-of-month 2})
  #_{:c-expr {:second [:+], :minute [:+ 1], :hour [:*], :day-of-month [:+ 2], :month [:*],
              :year [:*]},
     :board [:second :minute :hour :day-of-month :month :year],
     :mask [],
     :opts {},
     :top-recurrence :month}

  (read-c-expr {:month 2})
  #_{:c-expr {:second [:+], :minute [:+], :hour [:+], :day-of-month [:+], :month [:+ 2],
              :year [:*]},
     :board [:second :minute :hour :day-of-month :month :year],
     :mask [],
     :opts {},
     :top-recurrence :year}

  (read-c-expr {:month 2, :day-of-week :thu, :day-of-month [:+ [:* 1 14] [:* -7 -1]]})
  #_{:c-expr {:second [:+], :minute [:+], :hour [:+],
              :day-of-month [:+ [:* 1 14 1] [:* -7 -1 1]],
              :month [:+ 2],
              :year [:*],
              :day-of-week [:+ 4]},
     :board [:second :minute :hour :day-of-month :month :year],
     :mask [:day-of-week],
     :opts {},
     :top-recurrence :year}
  (read-c-expr {:minute 10, :hour 2, :day-of-week [1 2],
                :week-of-month [:+ [:* 1]]
                :month [:+ 3 5 10]
                :week-fields :iso})
  #_{:c-expr {:second [:+], :minute [:+ 10], :hour [:+ 2], :day-of-week [:+ 1 2],
              :week-of-month [:+ [:* 1]], :month [:+ 3 5 10], :year [:*]},
     :board [:second :minute :hour :day-of-week :week-of-month :month :year],
     :mask [],
     :opts {:week-fields :iso},
     :top-recurrence :year}
  )

;; Don't leak internal state to users of the library in case we want to refactor
(deftype -State [locale c-expr board satisfies-mask? iterators])

(defrecord Cronit [state current])

(defn- cronit-make [locale c-expr board satisfies-mask? iterators zdt]
  (->Cronit (->-State locale c-expr board satisfies-mask? iterators) zdt))

(defn- cronit-update [cronit iterators zdt]
  (let [^-State state (:state cronit)]
    (cronit-make (.locale state) (.c-expr state) (.board state) (.satisfies-mask? state)
                 iterators zdt)))

(defn locale
  "Return the `Locale` used by an iterator"
  [cronit]
  (.locale ^-State (:state cronit)))

(defn- step
  "Generic step function to move an interator forward or backward. Depending on the
  direction of the iteration and how fields wrap around, `gap-step-key` will be a keyword
  that tells where to resume after a gap (`:before-gap` or 38`:after-gap`), `iterator-step`
  is either `iterator-next` or `iterator-prev` to increment or decrement a time field,
  while `iterator-init-step` resets a time field to the first or last of its allowed
  values (`iterator-first` or `iterator-last`)."
  [cronit gap-step-key iterator-step iterator-init-step]
  (letfn [(rec-init [index iterators ldt]
            (if (< index 0)
              [iterators ldt]
              (let [iterator (iterator-reset (iterators index) ldt)
                    iterators (assoc iterators index iterator)]
                #_(dbg ["init" index iterator ldt])
                (if-let [[iterator ldt] (iterator-init-step iterator ldt)]
                  (recur (dec index) (assoc iterators index iterator) ldt)
                  (rec-step index iterators ldt)))))
          (rec-step [index iterators ldt]
            (let [iterator (iterators index)]
              #_(dbg ["step" index iterator ldt])
              (if-let [[iterator ldt] (iterator-step iterator ldt)]
                (rec-init (dec index) (assoc iterators index iterator) ldt)
                (recur (inc index) iterators ldt))))
          (rec-align [index iterators ldt]
            (if (< index 0)
              [iterators ldt]
              (let [iterator (iterators index)]
                (if (iterator-aligned? iterator)
                  (recur (dec index) iterators ldt)
                  (let [[iterators ldt] (rec-step index iterators ldt)]
                    (recur (dec index) iterators ldt))))))]
    (let [current ^ZonedDateTime (:current cronit)
          zone-id (.getZone current)
          state ^-State (:state cronit)
          board (.board state)
          satisfies-mask? (.satisfies-mask? state)]
      (loop [iterators (.iterators ^-State state),
             ldt (.toLocalDateTime ^ZonedDateTime current),
             aligned? (not (:unaligned? cronit))]
        (let [[iterators ldt] (if aligned?
                                (rec-step 0 iterators ldt)
                                (rec-align (dec (count board)) iterators ldt))
              zdt-or-gap (zone-ldt zone-id ldt)]
          (if-let [zdt (:zdt zdt-or-gap)]
            ;; not in a gap
            (if (satisfies-mask? zdt)
              (cronit-update cronit iterators zdt)
              (recur iterators ldt true))
            ;; in a gap => move to the end of the gap unless we are already there.
            (let [ldt (:after-gap zdt-or-gap)
                  zdt (:zdt (zone-ldt zone-id ldt))]
              (if (= current zdt)
                ;; We're already there. Test if next datetime outside gap is aligned or
                ;; recurse.
                (let [ldt (gap-step-key zdt-or-gap)
                      iterators (mapv (fn [it] (iterator-reset it ldt)) iterators)
                      aligned? (every? iterator-aligned? iterators)
                      zdt (:zdt (zone-ldt zone-id ldt))]
                  (if (and aligned? (satisfies-mask? zdt) (not= current zdt))
                    (cronit-update cronit iterators zdt)
                    (recur iterators ldt aligned?)))
                ;; First time we correct to the end of the gap (gap condition).
                (let [iterators (mapv (fn [it] (iterator-reset it ldt)) iterators)
                      aligned? (and (every? iterator-aligned? iterators)
                                    (satisfies-mask? zdt))]
                  (cond-> (cronit-update cronit iterators zdt)
                    (not aligned?) (assoc :unaligned? true
                                          :unaligned-cause :gap)))))))))))

(defn next
  "Return an iterator to the next time point. The iterator is
  tagged with with `{:unaligned?  true, :unaligned-cause :gap}` if its `:current`
  time point is not aligned with its specification because the next value felt
  within a gap when DST starts its current point was corrected to the end of the
  gap."
  [cronit]
  (step cronit :after-gap iterator-next iterator-first))

(defn prev
  "Return an iterator to the previous time point. The iterator is
  tagged with `{:unaligned? true, :unaligned-cause :gap}` if its `:current` time
  point is not aligned with its specification because the previous value felt
  within a gap when DST starts, it was not pointing to the gap alreay, and its
  current point was corrected to the end of the gap."
  [cronit]
  (step cronit :before-gap iterator-prev iterator-last))

(defn init
  "Return a cron iterator (cronit) for a cronit expression and initialized at a given point
  in time (`ZonedDateTime`). The iterator is a hashmap with internal `:state` and the
  `:current` time point. In addition, it is tagged with `{:unaligned?
  true, :unaligned-cause :init}` when the initial time point is not aligned with the
  specification given by the cronit expression."
  [c-expr ^ZonedDateTime time-point]
  (let [{:keys [:c-expr :board :mask :opts]} (read-c-expr c-expr)
        locale (locale-for (:locale opts))
        week-fields (week-fields-for (or (:week-fields opts) locale))
        week-field-key (some #{:week-of-month :week-of-year} board)
        iterators (mapv (fn [field-key]
                          (->field-iterator (tempo-get week-fields
                                                       (when (= field-key :day-of-week)
                                                         week-field-key)
                                                       field-key)
                                            (field-key c-expr)
                                            time-point))
                        board)
        satisfies-mask? (if (seq mask)
                          (let [tests
                                (mapv (fn [field-key]
                                        (let [contained? (containerp (c-expr field-key))
                                              {:keys [:bounds-fn :from-fn]}
                                              (tempo-get week-fields
                                                         (when (= field-key :day-of-week)
                                                           week-field-key)
                                                         field-key)]
                                          (fn [ldt]
                                            (let [{:keys [min max]} (bounds-fn ldt)]
                                              (contained? min max (from-fn ldt))))))
                                      mask)]
                            (fn [ldt] (every? (fn [test] (test ldt)) tests)))
                          (constantly true))
        cronit (cronit-make locale c-expr board satisfies-mask? iterators time-point)
        unaligned? (not (and (every? iterator-aligned? iterators)
                             (satisfies-mask? time-point)))
        gap? (and unaligned? (let [ldt (.toLocalDateTime time-point)
                                   trans (.getTransition
                                          (.getRules (.getZone time-point))
                                          (.minus ldt (Duration/ofMillis 1000)))]
                               (and trans
                                    (.isGap trans)
                                    (= (.getDateTimeAfter trans) ldt)
                                    (= (:current (next (prev cronit))) time-point))))]
    (cond gap? (assoc cronit :unaligned? true, :unaligned-cause :gap)
          unaligned? (assoc cronit :unaligned? true, :unaligned-cause :init)
          :else cronit)))

(defn valid?
  "Return `true` when an iterator is either aligned with its
  specification or is not aligned because of a DST gap condition. The `:current`
  position of an iterator can only be invalid once after a call to `init` because
  the initial time point is neither aligned nor meeting a gap condition already."
  [cronit]
  (or (not (:unaligned? cronit)) (= (:unaligned-cause cronit) :gap)))


;;;
;;; REPL utils
;;;

(set! *warn-on-reflection* false)

(defn show
  "Print generated time points for an expression around the current time. Options are
  `:context` to change the number of time points, `:format` to change the date format of
  the generated output (can be either a string, a `DateTimeFormatter` or `:iso`), `:date`
  to use a different reference date time (e.g. local \"2021-01-01T00:00\" or zoned), and
  `:zone-id` to override the system time zone with a different
  one (e.g. \"Europe/Brussels\") unless a zoned date has been provided."
  [c-expr & [{:keys [:date :context :format :zone-id] :or {context 12}}]]
  (let [zone-id (if zone-id
                  (ZoneId/of zone-id)
                  (ZoneId/systemDefault))
        zdt (if date
              (try (-> (LocalDateTime/parse date)
                       (.atZone zone-id))
                   (catch Exception _
                     (ZonedDateTime/parse date)))
              (ZonedDateTime/now))
        cronit (init c-expr zdt)
        formatter (-> (if (instance? DateTimeFormatter format)
                        format
                        (DateTimeFormatter/ofPattern
                         (cond (= format :iso)
                               "yyyy-MM-dd'T'HH:mm:ssxxx'['VV']'" ;not interested in µs
                               format format
                               :else
                               (if (= zone-id (ZoneId/systemDefault))
                                 "yyyy-MM-dd'T'HH:mm:ss E 'W'W 'w'ppw 'Q'Q"
                                 "yyyy-MM-dd'T'HH:mm:ssxxx'['VV']' E 'W'W 'w'ppw 'Q'Q"))))
                      (.withLocale (locale cronit)))]
    (letfn [(iter [step-fn cronit]
              (->> (iterate step-fn cronit)
                   (take (inc context))
                   (map :current)
                   (drop 1)))]
      (doseq [t (reverse (iter prev cronit))]
        (prn (.format t formatter)))
      (let [cursor (cond-> " ;<-"
                     (:unaligned-cause cronit) (str " " (:unaligned-cause cronit))
                     (valid? cronit) (str " " ":valid"))]
        (pr (.format (:current cronit) formatter))
        (println cursor))
      (doseq [t (iter next cronit)]
        (prn (.format t formatter))))))
