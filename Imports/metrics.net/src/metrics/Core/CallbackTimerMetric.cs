using System;
using System.Diagnostics;
using System.Runtime.Serialization;

namespace metrics.Core
{
   public class CallbackTimerMetric : TimerMetricBase
   {
      public CallbackTimerMetric(TimeUnit durationUnit, TimeUnit rateUnit)
         : base(durationUnit, rateUnit)
      {
      }

      public CallbackTimerMetric(
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
            var copy = new CallbackTimerMetric(
               DurationUnit, RateUnit, _meter, _histogram, false /* clear */
               );
            return copy;
         }
      }

      /// <summary>
      /// Starts recording an event. Call stop on the returned object (context) to finish the timing.
      /// </summary>
      /// <returns></returns>
      public CallbackTimerMetricContext Time()
      {
         var result = new CallbackTimerMetricContext(t => RecordElapsedTicks(t));
         result.Start();
         return result;
      }

      private void RecordElapsedTicks(long ticks)
      {
         Update(ticks * (1000L * 1000L * 1000L) / Stopwatch.Frequency);
      }

      public class CallbackTimerMetricContext
      {
         private readonly Stopwatch _stopwatch = new Stopwatch();
         private readonly Action<long> _ticksCallback;

         /// <summary>
         /// create a new context. pass the callback function that will be invoked with the elapsed ticks count
         /// </summary>
         /// <param name="ticksCallback"></param>
         internal CallbackTimerMetricContext(Action<long> ticksCallback)
         {
            _ticksCallback = ticksCallback;
         }


         internal void Start()
         {
            _stopwatch.Start();
         }

         public void Stop()
         {
            _stopwatch.Stop();
            _ticksCallback(_stopwatch.ElapsedTicks);
         }
      }

   }

}
