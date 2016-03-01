###0.2.16 / 2015-03-19
* retry starting httplistener on failure (better support for asp.net hosting where app pools are recycled)

###0.2.15 / 2015-03-03
* fix disposal of httplistener (@nkot)
* Added Process CPU usage into app counters (@o-mdr)
* resharper cleanup
* update dependencies

###0.2.14 / 2014-12-15
* fix possible issue when metrics are disabled and timer returns null TimerContext

###0.2.13 / 2014-12-14
* fix error when trying to globally disable metrics
* first elasticsearch bits

###0.2.11 / 2014-11-16
* graphite adapter (early bits, might have issues)
* refactor & cleanup reporting infra

###0.2.10 / 2014-11-06
* fix error logging for not found performance counters

###0.2.9 / 2014-11-05
* record active sessions for timers

###0.2.8 / 2014-10-29
* handle access issues for perf counters

###0.2.7 / 2014-10-28
* preparations for out-of-process metrics

###0.2.6 / 2014-10-17
* fix http listener prefix handling

###0.2.5 / 2014-10-12
* JSON metrics refactor
* remote metrics 

###0.2.4 / 2014-10-07
* JSON version
* added environment 
* various fixes

###0.2.3 / 2014-10-01
* add support for set counter & set meters [details](https://github.com/etishor/Metrics.NET/issues/21)
* cleanup owin adapter
* better & more resilient error handling

###0.2.2 / 2014-09-27
* add support for tagging metrics (not yet used in reports or visualization)
* add support for suppling a string user value to histograms & timers for tracking min / max / last values
* tests cleanup, some refactoring

###0.2.1 / 2014-09-25
* port latest changes from original metrics lib
* port optimization from ExponentiallyDecayingReservoir (https://github.com/etishor/Metrics.NET/commit/1caa9d01c16ff63504612d64771d52e9d7d9de5e)
* other minor optimizations
* add gauges for thread pool stats

###0.2.0 / 2014-09-20
* implement metrics contexts (and child contexts)
* make config more friendly
* most used condig options are now set by default
* add logging based on liblog (no fixed dependency - automaticaly wire into existing logging framework)
* update nancy & owin adapters to use contexts
* add some app.config settings to ease configuration

###0.1.11 / 2014-08-18
* update to latest visualization app (fixes checkboxes being outside dropdown)
* fix json caching in IE
* allow defining custom names for metric registry

###0.1.10 / 2014-07-30
* fix json formating (thanks to Evgeniy Kucheruk @kpoxa)

###0.1.9 / 2014-07-04
* make reporting more extensible

###0.1.8
* remove support for .NET 4.0

###0.1.6
* for histograms also store last value
* refactor configuration ( use Metric.Config.With...() )
* add option to completely disable metrics Metric.Config.CompletelyDisableMetrics() (useful for measuring metrics impact)
* simplify health checks
