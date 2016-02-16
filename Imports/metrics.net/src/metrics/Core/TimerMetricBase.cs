using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace metrics.Core
{
   public abstract class TimerMetricBase : IMetric, IMetered
   {
      protected MeterMetric _meter;
      protected HistogramMetric _histogram;

      public TimerMetricBase(TimeUnit durationUnit, TimeUnit rateUnit)
         : this(durationUnit, rateUnit, MeterMetric.New("calls", rateUnit), new HistogramMetric(HistogramMetric.SampleType.Biased), true /* clear */)
      {

      }

      protected TimerMetricBase(TimeUnit durationUnit, TimeUnit rateUnit, MeterMetric meter, HistogramMetric histogram, bool clear)
      {
         DurationUnit = durationUnit;
         RateUnit = rateUnit;
         _meter = meter;
         _histogram = histogram;
         if(clear)
         {
            Clear();
         }
      }

      public void LogJson(StringBuilder sb)
      {
          var percSb = new StringBuilder();
          foreach (var percentile in Percentiles(0.5, 0.75, 0.95, 0.98, 0.99, 0.999))
          {
              percSb.Append(" ").Append(percentile);
          }

          sb.Append("{\"count\":").Append(Count)
            .Append(",\"duration unit\":").Append(DurationUnit)
            .Append(",\"rate unit\":").Append(RateUnit)
            .Append(",\"fifteen minute rate\":").Append(FifteenMinuteRate)
            .Append(",\"five minute rate\":").Append(FiveMinuteRate)
            .Append(",\"one minute rate\":").Append(OneMinuteRate)
            .Append(",\"mean rate\":").Append(MeanRate)
            .Append(",\"max\":").Append(Max)
            .Append(",\"min\":").Append(Min)
            .Append(",\"mean\":").Append(Mean)
            .Append(",\"stdev\":").Append(StdDev)
            .Append(",\"percentiles\":").Append(percSb).Append("}");

      }
      /// <summary>
      ///  Returns the timer's duration scale unit
      /// </summary>
      public TimeUnit DurationUnit { get; private set; }

      /// <summary>
      /// Returns the meter's rate unit
      /// </summary>
      /// <returns></returns>
      public TimeUnit RateUnit { get; private set; }

      /// <summary>
      ///  Returns the number of events which have been marked
      /// </summary>
      /// <returns></returns>
      public long Count
      {
         get { return _histogram.Count; }
      }

      /// <summary>
      /// Returns the fifteen-minute exponentially-weighted moving average rate at
      /// which events have occured since the meter was created
      /// <remarks>
      /// This rate has the same exponential decay factor as the fifteen-minute load
      /// average in the top Unix command.
      /// </remarks> 
      /// </summary>
      public double FifteenMinuteRate
      {
         get { return _meter.FifteenMinuteRate; }
      }

      /// <summary>
      /// Returns the five-minute exponentially-weighted moving average rate at
      /// which events have occured since the meter was created
      /// <remarks>
      /// This rate has the same exponential decay factor as the five-minute load
      /// average in the top Unix command.
      /// </remarks>
      /// </summary>
      public double FiveMinuteRate
      {
         get { return _meter.FiveMinuteRate; }
      }

      /// <summary>
      /// Returns the mean rate at which events have occured since the meter was created
      /// </summary>
      public double MeanRate
      {
         get { return _meter.MeanRate; }
      }

      /// <summary>
      /// Returns the one-minute exponentially-weighted moving average rate at
      /// which events have occured since the meter was created
      /// <remarks>
      /// This rate has the same exponential decay factor as the one-minute load
      /// average in the top Unix command.
      /// </remarks>
      /// </summary>
      /// <returns></returns>
      public double OneMinuteRate
      {
         get { return _meter.OneMinuteRate; }
      }

      /// <summary>
      /// Returns the longest recorded duration
      /// </summary>
      public double Max
      {
         get { return ConvertFromNanos(_histogram.Max); }
      }

      /// <summary>
      /// Returns the shortest recorded duration
      /// </summary>
      public double Min
      {
         get { return ConvertFromNanos(_histogram.Min); }
      }

      /// <summary>
      ///  Returns the arithmetic mean of all recorded durations
      /// </summary>
      public double Mean
      {
         get { return ConvertFromNanos(_histogram.Mean); }
      }

      /// <summary>
      /// Returns the standard deviation of all recorded durations
      /// </summary>
      public double StdDev
      {
         get { return ConvertFromNanos(_histogram.StdDev); }
      }

      /// <summary>
      /// Returns the type of events the meter is measuring
      /// </summary>
      /// <returns></returns>
      public string EventType
      {
         get { return _meter.EventType; }
      }

      /// <summary>
      /// Returns a list of all recorded durations in the timers's sample
      /// </summary>
      public ICollection<double> Values
      {
         get
         {
            return _histogram.Values.Select(value => ConvertFromNanos(value)).ToList();
         }
      }

      /// <summary>
      /// Clears all recorded durations
      /// </summary>
      public void Clear()
      {
         _histogram.Clear();
      }

      public void Update(long duration, TimeUnit unit)
      {
         Update(unit.ToNanos(duration));
      }

      /// <summary>
      /// Returns an array of durations at the given percentiles
      /// </summary>
      public double[] Percentiles(params double[] percentiles)
      {
         var scores = _histogram.Percentiles(percentiles);
         for (var i = 0; i < scores.Length; i++)
         {
            scores[i] = ConvertFromNanos(scores[i]);
         }

         return scores;
      }

      protected void Update(long duration)
      {
         if (duration < 0) return;
         _histogram.Update(duration);
         _meter.Mark();
      }

      private double ConvertFromNanos(double nanos)
      {
         return nanos / DurationUnit.Convert(1, TimeUnit.Nanoseconds);
      }

      public abstract IMetric Copy { get; }
   }
}