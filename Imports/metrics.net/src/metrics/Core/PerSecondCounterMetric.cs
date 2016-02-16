using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using metrics.Stats;
using metrics.Support;
using System.Text;
using System.Runtime.Serialization;

namespace metrics.Core
{
    public class PerSecondCounterMetric : IMetric, IDisposable
    {
        private readonly string _eventType;
        private readonly TimeUnit _rateUnit;
        private readonly CancellationTokenSource _token = new CancellationTokenSource();


        private readonly EWMA _ewma = EWMA.OneSecondEWMA();


        private void TimeElapsed()
        {
            _ewma.Tick();
        }
        public void LogJson(StringBuilder sb)
        {
            sb.Append("{\"count\":").Append(CurrentValue)
              .Append(",\"rate unit\":").Append(RateUnit).Append("}");

        }
        [IgnoreDataMember]
        public IMetric Copy
        {
            get
            {
                var metric = new PerSecondCounterMetric(EventType, RateUnit);

                return metric;
            }
        }

        public void Mark(long n)
        {
            _ewma.Update(n);
        }

        public void Mark()
        {
            _ewma.Update(1);
        }

        public double CurrentValue
        {
            get { return _ewma.Rate(_rateUnit); }
        }

        public string EventType
        {
            get { return _eventType; }
        }

        public TimeUnit RateUnit
        {
            get { return _rateUnit; }
        }

        private PerSecondCounterMetric(string eventType, TimeUnit rateUnit)
        {
            _eventType = eventType;
            _rateUnit = rateUnit;
        }

        public static PerSecondCounterMetric New(string eventType)
        {
            var meter = new PerSecondCounterMetric(eventType, TimeUnit.Seconds);

            var interval = TimeSpan.FromSeconds(1);

            Task.Factory.StartNew(async () =>
            {
                while (!meter._token.IsCancellationRequested)
                {
                    await Task.Delay(interval, meter._token.Token);
                    meter.TimeElapsed();
                }
            }, meter._token.Token);

            return meter;
        }

        public void Dispose()
        {
            _token.Cancel();
        }
    }
}