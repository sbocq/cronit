# Cronit

Cronit (short for cron iterator) is a Clojure library to iterate over the time
points defined by a cron like expression.


## Features and rationale

**Small and free of side effects.**

The library is small, and is free of side effects. It does not assume a specific
threading nor a persistence scheme. It is just meant to handle all the date time
specific aspects needed to iterate forward or backward over recurrent time points
derived from a cron like expression. Possible applications are for example a
periodic job scheduling engine or a calendar that displays recurrent events on a
time grid around a given point in time.

**Structured expressions, not strings.**

Recurrent time points are defined using a hashmap with time fields as keys and
tagged vector as values, where the tag `:+` denotes an enumeration and `:*` a
range expression. For example, the expression `{:hour [:* 2], :day-of-week [:+
:mon :wed]}` defines time points that occur every 2 hour on Monday and
Wednesdays. It is equivalent to the standard cron expression `0 */2 * * mon,wed`.

**Values relative to the end of a range.**

Values relative to the end of a temporal range can here be expressed using
negative offsets, which addresses a known limitation with cron. For example,
`{:day-of-month [:+ -1]}` will pick the the last day of every month, and
`{:day-of-month [:* -7 -1], :day-of-week [:+ :thu]}` will pick the last Thursday
of every month, by combining the last 7 days of any month with any Thursday of a
week.

**Many time fields.**

The library relies on `java.time` to adjust date and times of temporal
fields. Since `java.time` supports many kinds of temporal fields that fit the
same abstraction, it felt natural to support more fields than those available in
standard cron, such as `:second`, `:week-of-month`, `:week-of-year`,
`day-of-quarter`, `quarter-of-year`, and `day-of-year` fields.

## Installation

Following the link below for instructions:

[![Clojars Project](https://img.shields.io/clojars/v/org.clojars.sbocq/cronit.svg)](https://clojars.org/org.clojars.sbocq/cronit)


# Usage

Import the library in a project:

```clojure
(require [sbocq.cronit :as c])
```

Create a new iterator for every 12th hours in a day, on Monday and Wednesdays,
initialized on the 11th June 2021 at 11h15m30:

```clojure
(c/init {:hour [:* 12], :day-of-week [:+ :mon :wed]}
        (ZonedDateTime/parse "2021-06-16T11:15:30+02:00[Europe/Brussels]"))
{..., :current #object[java.time.ZonedDateTime 0x46dc45c3 "2021-06-16T11:15:30+02:00[Europe/Brussels]"]}
```

Iterate to the third time point after the initial position:

```clojure
(-> (c/init {:hour [:* 12], :day-of-week [:+ :mon :wed]}
            (ZonedDateTime/parse "2021-06-16T11:15:30+02:00[Europe/Brussels]"))
    c/next c/next c/next :current str)
"2021-06-21T12:00+02:00[Europe/Brussels]"
```

Iterate to the third time point before the initial position:

```clojure
(-> (c/init {:hour [:* 12], :day-of-week [:+ :mon :wed]}
            (ZonedDateTime/parse "2021-06-16T11:15:30+02:00[Europe/Brussels]"))
    c/prev c/prev c/prev :current str)
"2021-06-14T00:00+02:00[Europe/Brussels]"
```

Print valid time points for an expression around a date for some locale that uses ISO week fields definition:

```clojure
(c/show {:day-of-month 1, :locale "en-be"}
        {:date "2021-07-07T12:00" :context 3, :zone-id "Europe/Brussels"})
"2021-05-01T00:00:00 Sat W0 w17 Q2"
"2021-06-01T00:00:00 Tue W1 w22 Q2"
"2021-07-01T00:00:00 Thu W1 w26 Q3"
"2021-07-07T12:00:00 Wed W2 w27 Q3" <- :init
"2021-08-01T00:00:00 Sun W0 w30 Q3"
"2021-09-01T00:00:00 Wed W1 w35 Q3"
"2021-10-01T00:00:00 Fri W0 w39 Q4"
```

**Runnable examples**

The source repository contains also examples that can be executed from the
command line:

- Show a time grid for an expression in a locale that uses ISO week fields definition:

``` shell
    $ clj -X:examples timegrid/show \
        :context 12 \
        :c-expr '{:hour [:* 2], :day-of-week [:+ :mon :wed], :locale "fr-be"}' \
        :date '"2021-01-01T00:00"'
```

- Solve a time variant of FizzBuzz by scheduling Fizz every 3 seconds and Buzz
every 5 seconds:

```shell
    $ clj -X:examples scheduler/run \
        :timers '["fizz" {:second [:* 3]} "buzz" {:second [:* 5]}]'
```


# Reference

## API

- `(init c-expr time-point)` Return a cron iterator for a cronit expression and
  initialized at a given point in time (`ZonedDateTime`). The iterator is a
  hashmap with internal `:state` and the `:current` time point. In addition, it
  is tagged with `{:unaligned?  true, :unaligned-cause :init}` when the initial
  time point is not aligned with the specification given by the cronit
  expression.

- `(next cronit)` Return an iterator to the next time point. The iterator is
  tagged with with `{:unaligned?  true, :unaligned-cause :gap}` if its `:current`
  time point is not aligned with its specification because the next value felt
  within a gap when DST starts its current point was corrected to the end of the
  gap.

- `(prev cronit)` Return an iterator to the previous time point. The iterator is
  tagged with `{:unaligned? true, :unaligned-cause :gap}` if its `:current` time
  point is not aligned with its specification because the previous value felt
  within a gap when DST starts, it was not pointing to the gap alreay, and its
  current point was corrected to the end of the gap.

- `(valid? cronit)` Return `true` when an iterator is either aligned with its
  specification or is not aligned because of a DST gap condition. The `:current`
  position of an iterator can only be invalid once after a call to `init` because
  the initial time point is neither aligned nor meeting a gap condition already.

- `(locale cronit)` Return the `Locale` used by an iterator.

##### REPL utils


- `(show c-expr & [{:keys [:context :format :date :zone-id] :or {context 12}}])`
  Print generated time points for an expression around the current time. Options
  are `:context` to change the number of time points, `:format` to change the
  date format of the generated output (can be either a string, a
  `DateTimeFormatter` or `:iso`), `:date` to use a different reference date time
  (e.g. local `"2021-01-01T00:00"` or zoned), and `:zone-id` to override the system
  time zone with a different one (e.g. "Europe/Brussels") unless a zoned date has
  been provided.


## Expressions

#### Cronit expression


A hashmap `{<field-key> <field-value>}` where:
- `field-key := :second | :minute | :hour | :day-of-week | :week-of-month | :month | :week-of-year | :day-of-month | :day-of-quarter | :quarter-of-year | :day-of-year`
- `field-value := <range-expression> | < enum-expression>` (see below)

The hasmap can be augmented with the following options:

- `:locale` : specify the locale tag used to interpret week fields.

**Example**

``` clojure
{:minute 1
 :hour [:* 10 14]
 :day-of-week [:+ :sun]
 :day-of-month [:+ [:* 1 14] [:* -7 -1]]})
```


#### Range expressions (tag `:*`)

Range expressions capture field values that are incremented by `<step>` units in a fixed
range from `<min>` to, and including, a `<max>` value.

Its bounded form is:

     [:* <min> <max> <step>]

where `min` or `max` are inclusive.

Its unbounded form is:

     [:* <step>]

in which case the `min` and `max` bounds will be inferred to the largest range at
a given time point, (see
[`TemporalField#rangeRefinedBy`](https://docs.oracle.com/javase/8/docs/api/java/time/temporal/TemporalField.html#rangeRefinedBy-java.time.temporal.TemporalAccessor-)). For
example, with `{:day-of-month [:* 1]}`, the interval inferred is `[1, 29]` in the
month of February of leap years. The range is reset on the next month, therefore
both the 29th of February and the 1st of March are valid dates for this set. With
`{:day-of-year [:*]}`, the valid range would be reset at the beginning of the
each year. Even when they are specified, the bounds are clipped to the valid
range given by the upper field. For example, `{:day-of-month [:* 0 31 2]}` will
be corrected to `{:day-of-month [:* 1 30 2]}` in April.

The following syntactic sugar is supported for range expressions:

     [:* <min> <max>] ==> [:* <min> <max> 1]

     :* ==> [:*] <==> [:* 1]


Bounds can be negative offsets that express values relative to the end of the
period in which a date is valid, plus one. For example, `[:* -7 -1]` represents the
7 last possible value a field can take at a given point in time, and the expression
`{:day-of-month [:* -7 -1]}` will match the last 7 days of every month.


#### Enumeration expressions (tag: `:+`)

Enumeration expressions enumerate all the allowed values a field can take.

Its standard form is:

     [:+ <v_1> ... <v_n>]

where `<v_j>` is either:
- a number,
- a supported keyword for a field value (see below).
- or a range expression.

In its empty form:

     [:+]

the enumeration `[:+ <min>]` is inferred, where `<min>` is the lowest bound of
the corresponding time field at the given time point.

The following syntactic sugar is supported:

     v ==> [:+ v]

     :+ ==> [:+]

Values can be negative offsets that express values relative to the end of the
period in which a date is valid, plus one.  For example, `-1` represents the last
possible value a field can take at a given point in time, and the expression
`{:day-of-month -1}` will match the last day of every month.


#### Supported field value keywords

Values of `:day-of-week` and `:month-of-year` can be specified using keywords
instead of integers.

- `:day-of-week` value keywords:

       :mon :tue :wed :thu :fri :sat :sun

- `:month-of-year` value keywords:

       :jan :feb :mar :apr :may :jun :jul :aug :sep :oct :nov :dec

It is recommended to always use keywords for `:day-of-week` instead of integers
because the integer offset of the first day will differ depending the locale.


#### Inferrence of missing field

Missing fields that have a smaller unit than the first field provided by the
client are inferred as the first value of their range, while those that have a
greater unit are inferred as the range of all their possible value. For example,
the library will expand the expression `{:hour [:* 12], :day-of-week [:+ :mon
:wed]}` to `{:second [:+], :minute [:+], :hour [:* 2], :day-of-week [:+ 1 3],
:week-of-year [:*], :year [:*]}`.

To do so, the library first attempts to fit the provided expression with one of
these possible boards:

``` clojure
c/boards
[[:second :minute :hour :day-of-year]
 [:second :minute :hour :day-of-quarter :quarter-of-year]
 [:second :minute :hour :day-of-month :month]
 [:second :minute :hour :day-of-week :week-of-year]
 [:second :minute :hour :day-of-week :week-of-month :month]]
```

When all fields do not fit on a board, the library use the remaining fields as an
"AND" `mask` and time points that satisfy neither the board nor the mask
expressions will be skiped.

The result of the inference can be observed using the `boards-for` function, for
example:

``` clojure
(c/board-for {:minute 1
              :day-of-week [:+ :sun]
              :day-of-month [:+ [:* 1 14] [:* -7 -1]]})
{:board [:second :minute :hour :day-of-week :week-of-year], :mask [:day-of-month]}
```

## Other considerations


#### Daylight Saving Time (DST)


As seen in the examples above, the library uses `java.time.ZonedDateTime` to
identify the time points of recurrent events and it will simulate the
behavior of regular cron jobs during Daylight Saving Time (DST) transitions.

For zones with daylight saving time, the behavior is the following:

- Time points that fall in a gap when DST starts are collapsed to a single time
  point that is set at the end of the gap.

- When DST stops and there is an overlap, time points are returned once and in
  the earliest zone offset.

This is illustrated in the examples below using Brussels time zone for which DST
starts on March 28th at 2am and stops on October 31st at 3am in 2021:

```clojure
(c/show {:minute [:* 30], :hour [:+ 1 2 4], :day-of-month [:+ 28], :month [+ 3 10]}
        {:date "2021-01-01T00:00", :zone-id "Europe/Brussels" :context 4})
"2020-10-28T02:00:00 Wed W5 w44 Q4"
"2020-10-28T02:30:00 Wed W5 w44 Q4"
"2020-10-28T04:00:00 Wed W5 w44 Q4"
"2020-10-28T04:30:00 Wed W5 w44 Q4"
"2021-01-01T00:00:00 Fri W1 w 1 Q1" ;<- :init
"2021-03-28T01:00:00 Sun W5 w14 Q1"
"2021-03-28T01:30:00 Sun W5 w14 Q1"
"2021-03-28T03:00:00 Sun W5 w14 Q1"
"2021-03-28T04:00:00 Sun W5 w14 Q1"

(c/show {:minute [:* 30], :hour [:+ 2 3 4], :day-of-month [:+ 31], :month [:+ 10]}
        {:date "2021-01-01T00:00", :format :iso, :zone-id "Europe/Brussels" :context 5})
"2020-10-31T02:30:00+01:00[Europe/Brussels]"
"2020-10-31T03:00:00+01:00[Europe/Brussels]"
"2020-10-31T03:30:00+01:00[Europe/Brussels]"
"2020-10-31T04:00:00+01:00[Europe/Brussels]"
"2020-10-31T04:30:00+01:00[Europe/Brussels]"
"2021-01-01T00:00:00+01:00[Europe/Brussels]" ;<- :init
"2021-10-31T02:00:00+02:00[Europe/Brussels]"
"2021-10-31T02:30:00+02:00[Europe/Brussels]"
"2021-10-31T03:00:00+01:00[Europe/Brussels]"
"2021-10-31T03:30:00+01:00[Europe/Brussels]"
"2021-10-31T04:00:00+01:00[Europe/Brussels]"
```


#### Week fields, Locale, and day of week boundaries

The first thing to know about
[`java.time.temporal.WeekFields`](https://docs.oracle.com/javase/8/docs/api/java/time/temporal/WeekFields.html)
is that their values are offsets dependent on the locale. So depending on the
locale, week `0` may not exist, the first week must have at least `1`, `4` or `5`
days, or the first day of a week may begin on Sunday or on Monday.

The week offset can be made independent from the locale by using keywords for
each day instead of integers. But for week of year or week of month, there is no
good solution, and one must use them wisely.

The second thing to be aware of is that the first or last week of a year may
contain days from the previous or next calendar year. And although it is not
explicitly mentioned in the documentation, the same goes for week of months.

This can be seen in the example below. If we ask for the first Sunday of the
first week of year 2022, or of month of January 2022, using the `SUNDAY_START`
definition of week fields, then we get the last Sunday of December 2021.

``` clojure
(as-> (LocalDateTime/parse "2022-04-01T00:00") t
  (.adjustInto (.weekOfYear WeekFields/SUNDAY_START) t 1)
  (.adjustInto (.dayOfWeek WeekFields/SUNDAY_START) t 1)
  (str t))
"2021-12-26T00:00"

(as-> (LocalDateTime/parse "2022-04-01T00:00") t
  (.adjustInto ChronoField/MONTH t 1)
  (.adjustInto (.weekOfMonth WeekFields/SUNDAY_START) t 1)
  (.adjustInto (.dayOfWeek WeekFields/SUNDAY_START) t 1)
  (str t))
"2021-12-26T00:00"

(as-> (LocalDateTime/parse "2021-12-26T00:00") t
  (.getFrom (.weekOfYear WeekFields/SUNDAY_START) t))
53

(as-> (LocalDateTime/parse "2021-12-26T00:00") t
  (.getFrom (.weekOfMonth WeekFields/SUNDAY_START) t))
5

(-> (LocalDateTime/parse "2021-12-26T00:00")
    (.format (-> (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss E 'W'W 'w'ppw 'Q'Q")
                 (.withLocale (Locale/forLanguageTag "en-us")))))
"2021-12-26T00:00:00 Sun W5 w 1 Q4"
```

The problem is that it results into some ambiguities when the last week of one
year is also the first week of the next year. A cron expression asking for every
last and first Sunday of the first or last week of the year may return twice the
same day. First, it would be unexpected behavior for a cron job to run twice, and
even then, during initialization, it won't be able to disambiguate whether Sunday
26th December 2021 should be considered as the last Sunday of 2021 or the first
Sunday of 2022.

The library solves the ambiguity by restricting days in the first or last week of
a month or a year to the days in that same month or year. One important
consequence, or caveat, is that some days may not exist in a given week of a
given month or year. For example, since the first Sunday of the first week of the
year 2021 is in 2020 and the library requires that all days of 2021 stay in 2021
to avoid the ambiguity with the last Sunday of 2020, the first Sunday of 2021
does not exist but the last one of 2020 does. This is illustrated below for 2021
and other years:

``` clojure
(c/show {:day-of-week :sun, :week-of-year :+, :locale "en-us"}
        {:date "2021-07-01T00:00", :context 6})
"1984-01-01T00:00:00 Sun W1 w 1 Q1"
"1989-01-01T00:00:00 Sun W1 w 1 Q1"
"1995-01-01T00:00:00 Sun W1 w 1 Q1"
"2006-01-01T00:00:00 Sun W1 w 1 Q1"
"2012-01-01T00:00:00 Sun W1 w 1 Q1"
"2017-01-01T00:00:00 Sun W1 w 1 Q1"
"2021-07-01T00:00:00 Thu W1 w27 Q3" ;<- :init
"2023-01-01T00:00:00 Sun W1 w 1 Q1"
"2034-01-01T00:00:00 Sun W1 w 1 Q1"
"2040-01-01T00:00:00 Sun W1 w 1 Q1"
"2045-01-01T00:00:00 Sun W1 w 1 Q1"
"2051-01-01T00:00:00 Sun W1 w 1 Q1"
"2062-01-01T00:00:00 Sun W1 w 1 Q1"

(c/show {:day-of-week :sun, :week-of-year -1, :locale "en-us"}
        {:date "2021-07-01T00:00", :context 6})
"2015-12-27T00:00:00 Sun W5 w 1 Q4"
"2016-12-25T00:00:00 Sun W5 w53 Q4"
"2017-12-31T00:00:00 Sun W6 w 1 Q4"
"2018-12-30T00:00:00 Sun W6 w 1 Q4"
"2019-12-29T00:00:00 Sun W5 w 1 Q4"
"2020-12-27T00:00:00 Sun W5 w 1 Q4"
"2021-07-01T00:00:00 Thu W1 w27 Q3" ;<- :init
"2021-12-26T00:00:00 Sun W5 w 1 Q4"
"2022-12-25T00:00:00 Sun W5 w53 Q4"
"2023-12-31T00:00:00 Sun W6 w 1 Q4"
"2024-12-29T00:00:00 Sun W5 w 1 Q4"
"2025-12-28T00:00:00 Sun W5 w 1 Q4"
"2026-12-27T00:00:00 Sun W5 w 1 Q4"
```

All this to conclude that weeks of month or year are best used with caution, or
at least not in conjunction with a specific day of the week. It behaves as
expected when one lets the library pick the first day of the week like shown
below:

``` clojure
(c/show {:week-of-year :+, :locale "en-us"}
        {:date "2021-07-01T00:00", :context 4})
"2018-01-01T00:00:00 Mon W1 w 1 Q1"
"2019-01-01T00:00:00 Tue W1 w 1 Q1"
"2020-01-01T00:00:00 Wed W1 w 1 Q1"
"2021-01-01T00:00:00 Fri W1 w 1 Q1"
"2021-07-01T00:00:00 Thu W1 w27 Q3" ;<- :init
"2022-01-01T00:00:00 Sat W1 w 1 Q1"
"2023-01-01T00:00:00 Sun W1 w 1 Q1"
"2024-01-01T00:00:00 Mon W1 w 1 Q1"
"2025-01-01T00:00:00 Wed W1 w 1 Q1"

(c/show {:week-of-year -1, :locale "en-us"}
        {:date "2021-07-01T00:00", :context 6})
"2017-12-31T00:00:00 Sun W6 w 1 Q4"
"2018-12-30T00:00:00 Sun W6 w 1 Q4"
"2019-12-29T00:00:00 Sun W5 w 1 Q4"
"2020-12-27T00:00:00 Sun W5 w 1 Q4"
"2021-07-01T00:00:00 Thu W1 w27 Q3" ;<- :init
"2021-12-26T00:00:00 Sun W5 w 1 Q4"
"2022-12-25T00:00:00 Sun W5 w53 Q4"
"2023-12-31T00:00:00 Sun W6 w 1 Q4"
"2024-12-29T00:00:00 Sun W5 w 1 Q4"

(c/show {:week-of-year :+, :locale "en-be"}
        {:date "2021-07-01T00:00", :context 4})
"2018-01-01T00:00:00 Mon W1 w 1 Q1"
"2019-01-01T00:00:00 Tue W1 w 1 Q1"
"2020-01-01T00:00:00 Wed W1 w 1 Q1"
"2021-01-01T00:00:00 Fri W0 w53 Q1"
"2021-07-01T00:00:00 Thu W1 w26 Q3" ;<- :init
"2022-01-01T00:00:00 Sat W0 w52 Q1"
"2023-01-01T00:00:00 Sun W0 w52 Q1"
"2024-01-01T00:00:00 Mon W1 w 1 Q1"
"2025-01-01T00:00:00 Wed W1 w 1 Q1"

(c/show {:week-of-year -1, :locale "en-be"}
        {:date "2021-07-01T00:00", :context 4})
"2017-12-25T00:00:00 Mon W4 w52 Q4"
"2018-12-31T00:00:00 Mon W5 w 1 Q4"
"2019-12-30T00:00:00 Mon W5 w 1 Q4"
"2020-12-28T00:00:00 Mon W5 w53 Q4"
"2021-07-01T00:00:00 Thu W1 w26 Q3" ;<- :init
"2021-12-27T00:00:00 Mon W5 w52 Q4"
"2022-12-26T00:00:00 Mon W5 w52 Q4"
"2023-12-25T00:00:00 Mon W4 w52 Q4"
"2024-12-30T00:00:00 Mon W5 w 1 Q4"
```

And then, it is not ambiguous, and probably a more frequent question to ask for
the first Sunday of the year, instead of the first Sunday of the first week of
the year, which is easily expressed like this:

``` clojure
(c/show {:day-of-week :sun, :day-of-year [:* 1 7], :locale "en-us"}
        {:date "2021-07-01T00:00", :context 6})
"2016-01-03T00:00:00 Sun W2 w 2 Q1"
"2017-01-01T00:00:00 Sun W1 w 1 Q1"
"2018-01-07T00:00:00 Sun W2 w 2 Q1"
"2019-01-06T00:00:00 Sun W2 w 2 Q1"
"2020-01-05T00:00:00 Sun W2 w 2 Q1"
"2021-01-03T00:00:00 Sun W2 w 2 Q1"
"2021-07-01T00:00:00 Thu W1 w27 Q3" ;<- :init
"2022-01-02T00:00:00 Sun W2 w 2 Q1"
"2023-01-01T00:00:00 Sun W1 w 1 Q1"
"2024-01-07T00:00:00 Sun W2 w 2 Q1"
"2025-01-05T00:00:00 Sun W2 w 2 Q1"
"2026-01-04T00:00:00 Sun W2 w 2 Q1"
"2027-01-03T00:00:00 Sun W2 w 2 Q1"
```


#### Other differences with standard cron

A last note is that the library is ANDing fields with the same unit such as day
of month and day of week, while standard cron is ORing these fields.


## Misc.

Run the project's tests:

    $ clojure -M:test:runner

Build the jar:

    $ clj -X:build clean
    $ clj -X:build jar

## Copyright & License

The MIT License (MIT)

Copyright © 2021-2021 Sébastien Bocq.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
