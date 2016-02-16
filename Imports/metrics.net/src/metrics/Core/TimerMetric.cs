using System;
using System.Diagnostics;
using System.Runtime.Serialization;


namespace metrics.Core
{
   /// <summary>
    /// A timer metric which aggregates timing durations and provides duration
    /// statistics, plus throughput statistics via <see cref="MeterMetric" />.
    /// </summary>
    public class TimerMetric : TimerMetricBase
    {
      public TimerMetric(TimeUnit durationUnit, TimeUnit rateUnit)
            : base(durationUnit, rateUnit)
        {
        }

        public TimerMetric(TimeUnit durationUnit, TimeUnit rateUnit, MeterMetric meter, HistogramMetric histogram, bool clear) : base(durationUnit, rateUnit, meter, histogram, clear)
        {
        }

        [IgnoreDataMember]
        public override IMetric Copy
        {
           get
           {
              var copy = new TimerMetric(
                 DurationUnit, RateUnit, _meter, _histogram, false /* clear */
                 );
              return copy;
           }
        }

      /// <summary>
        /// Times and records the duration of an event
        /// </summary>
        /// <typeparam name="T">The type of the value returned by the event</typeparam>
        /// <param name="event">A function whose duration should be timed</param>
        public T Time<T>(Func<T> @event)
        {
            var stopwatch = new Stopwatch();
            try
            {
                stopwatch.Start();
                return @event.Invoke();
            }
            finally
            {
                stopwatch.Stop();
                Update(stopwatch.ElapsedTicks * (1000L * 1000L * 1000L) / Stopwatch.Frequency);
            }
        }

        /// <summary>
        /// Times and records the duration of an event
        /// </summary>
        /// <param name="event">An action whose duration should be timed</param>
        public void Time(Action @event)
        {
            Time(() =>
                     {
                         @event();
                         return null as object;
                     });
        }
    }
}