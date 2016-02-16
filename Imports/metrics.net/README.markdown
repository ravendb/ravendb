Metrics.NET
===========

*Capturing CLR and application-level metrics. So you know what's going on.*

####(This work began as a port of @codahale's [metrics](http://github.com/codahale/metrics) for Scala and the JVM.)

Introduction
------------
In a post-agile world, we are asked to look beyond the technologies that enable our practice, and find ways to ensure 
the choices we make are informed by customers and stand up to reality. Experiment-driven (or evidence-based) development
is a way of combining run-time metrics with automated experiments, resulting in software that is “natural”, based on 
actual use and runtime performance rather than the strongest opinion.

This library fulfills the run-time aspect for practicing EDD in a .NET development environment.

Requirements
------------
* .NET 4.0 (reporting via HTTP available via `MetricsListener` class)
* ASP.NET MVC 3 (reporting via HTTP available via route registrations)

How To Use
----------
**First**, specify Metrics as a dependency:

    PM> Install-Package Metrics
    PM> Install-Package Metrics.Mvc

**Second**, instrument your classes:

```csharp
using Metrics;

public class ThingFinder
{
    // Measure the # of records per second returned
    private IMetric _resultsMeter = Metrics.Meter(typeof(ThingFinder), "results", TimeUnit.Seconds)
  
    // Measure the # of milliseconds each query takes and the number of queries per second being performed
    private IMetric _dbTimer = Metrics.Timer(typeof(ThingFinder), "database", TimeUnit.Milliseconds, TimeUnit.Seconds)
  
    public void FindThings()
    {
        // Perform an action which gets timed
        var results = _dbTimer.Time(() => {                            
            Database.Query("SELECT Unicorns FROM Awesome");
        }

        // Calculate the rate of new things found
        _resultsMeter.Mark(results.Count)                
    
        // etc.
    }
}
```

Metrics comes with five types of metrics:

* **Gauges** are instantaneous readings of values (e.g., a queue depth).
* **Counters** are 64-bit integers which can be incremented or decremented.
* **Meters** are increment-only counters which keep track of the rate of events.
  They provide mean rates, plus exponentially-weighted moving averages which
  use the same formula that the UNIX 1-, 5-, and 15-minute load averages use.
* **Histograms** capture distribution measurements about a metric: the count,
  maximum, minimum, mean, standard deviation, median, 75th percentile, 95th
  percentile, 98th percentile, 99th percentile, and 99.9th percentile of the
  recorded values. (They do so using a method called reservoir sampling which
  allows them to efficiently keep a small, statistically representative sample
  of all the measurements.)
* **Timers** record the duration as well as the rate of events. In addition to
  the rate information that meters provide, timers also provide the same metrics
  as histograms about the recorded durations. (The samples that timers keep in
  order to calculate percentiles and such are biased towards more recent data,
  since you probably care more about how your application is doing *now* as
  opposed to how it's done historically.)

Metrics also has support for health checks:
```csharp
HealthChecks.Register("database", () =>
{
    if (Database.IsConnected)
    {
        return HealthCheck.Healthy;
    }
    else
    {
        return HealthCheck.Unhealthy("Not connected to database");
    }
});
```

**Third**, start collecting your metrics.

If you're simply running a benchmark, you can print registered metrics to 
standard output, every 10 seconds like this:

```csharp
// Print to Console.Out every 10 seconds
Metrics.EnableConsoleReporting(10, TimeUnit.Seconds) 
```

If you're writing a ASP.NET MVC-based web service, you can reference `Metrics.AspNetMvc` in
your web application project and register default routes:

```csharp
using metrics;

public class MvcApplication : HttpApplication
{
	// ...
	
	protected void Application_Start()
	{
		AspNetMvc.Metrics.RegisterRoutes();
		
		// ...            
	}

	// ...
}
```
    
The default routes will respond to the following URIs:

* `/metrics`: A JSON object of all registered metrics and a host of CLR metrics.
* `/ping`: A simple `text/plain` "pong" for load-balancers.
* `/healthcheck`: Runs through all registered `HealthCheck` instances and reports the results. Returns a `200 OK` if all succeeded, or a `500 Internal Server Error` if any failed.
* `/threads`: A `text/plain` dump of all threads and their stack traces.

The URIs of these resources can be configured by setting properties prior to registering routes.
You may also choose to protect these URIs with HTTP Basic authentication:

```csharp
using metrics;

public class MvcApplication : HttpApplication
{
	// ...
	
	protected void Application_Start()
	{
		AspNetMvc.Metrics.HealthCheckPath = "my-healthcheck-uri";
		AspNetMvc.Metrics.PingPath = "my-ping-uri";
		AspNetMvc.Metrics.MetricsPath = "my-metrics-uri";
		AspNetMvc.Metrics.ThreadsPath = "my-threads-uri";

		AspNetMvc.Metrics.RegisterRoutes("username", "password");
		
		// ...            
	}

	// ...
}
```

Known Deviations
----------------
* This implementation uses `ConcurrentDictionary` vs. Java's `ConcurrentSkipListMap`, so expect lookups to suffer
* This implementation uses `SortedDictionary` vs. Java's `TreeMap`
* The CLR is not as flexible when it comes to introspection; CLR metrics and thread dumps are a work in progress, but are largely based on PerformanceCounters
		
License
-------
The original Metrics project is Copyright (c) 2010-2011 Coda Hale, Yammer.com

This idiomatic port of Metrics to C# and .NET is Copyright (c) 2011 Daniel Crenna

Both works are published under The MIT License, see LICENSE