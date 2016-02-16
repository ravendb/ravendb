using System.Runtime.Serialization;

namespace metrics.Core
{
   public class ManualTimerMetric : TimerMetricBase
   {
      public ManualTimerMetric(TimeUnit durationUnit, TimeUnit rateUnit) : base(durationUnit, rateUnit)
      {
      }

      public ManualTimerMetric(
         TimeUnit durationUnit, 
         TimeUnit rateUnit, 
         MeterMetric meter, 
         HistogramMetric histogram, 
         bool clear)
         : base(durationUnit, rateUnit, meter, histogram, clear)
      {
      }

      [IgnoreDataMember]
      public override IMetric Copy
      {
         get
         {
            var copy = new ManualTimerMetric(
               DurationUnit, RateUnit, _meter, _histogram, false /* clear */
               );
            return copy;
         }
      }

      public void RecordElapsedMillis(long milliSeconds)
      {
         Update(milliSeconds, TimeUnit.Milliseconds);
      }
   }
}
