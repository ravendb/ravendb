
using System;
namespace Metrics.Utils
{
    /// <summary>
    /// An exponentially-weighted moving average.
    /// <a href="http://www.teamquest.com/pdfs/whitepaper/ldavg1.pdf">UNIX Load Average Part 1: How It Works</a>
    /// <a href="http://www.teamquest.com/pdfs/whitepaper/ldavg2.pdf">UNIX Load Average Part 2: Not Your Average Average</a>
    /// <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">EMA</a>
    /// </summary>
    public class EWMA
    {
        private const int Interval = 5;
        private const double SecondsPerMinute = 60.0;
        private const int OneMinute = 1;
        private const int FiveMinutes = 5;
        private const int FifteenMinutes = 15;
        private static readonly double M1Second = 1.0 - Math.Exp(-1.0);
        private static readonly double M1Alpha = 1 - Math.Exp(-Interval / SecondsPerMinute / OneMinute);
        private static readonly double M5Alpha = 1 - Math.Exp(-Interval / SecondsPerMinute / FiveMinutes);
        private static readonly double M15Alpha = 1 - Math.Exp(-Interval / SecondsPerMinute / FifteenMinutes);
        

        private volatile bool initialized = false;
        private VolatileDouble rate = new VolatileDouble(0.0);

        private AtomicLong uncounted = new AtomicLong();
        private readonly double alpha;
        private readonly double interval;

        public static EWMA OneSecondEWMA()
        {
            return new EWMA(M1Second, 1);
        }

        public static EWMA OneMinuteEWMA()
        {
            return new EWMA(M1Alpha, Interval);
        }

        public static EWMA FiveMinuteEWMA()
        {
            return new EWMA(M5Alpha, Interval);
        }

        public static EWMA FifteenMinuteEWMA()
        {
            return new EWMA(M15Alpha, Interval);
        }

        public EWMA(double alpha, long interval)
        {
            this.interval = interval*Clock.NANOSECONDS_IN_SECOND;
            this.alpha = alpha;
        }

        public void Update(long value)
        {
            uncounted.Add(value);
        }

        public void Tick()
        {
            long count = uncounted.GetAndReset();

            double instantRate = count / interval;
            if (initialized)
            {
                double doubleRate = rate.Get();
                rate.Set(doubleRate + alpha * (instantRate - doubleRate));
            }
            else
            {
                rate.Set(instantRate);
                initialized = true;
            }
        }

        public double GetRate()
        {
            return rate.Get() *Clock.NANOSECONDS_IN_SECOND;
        }

        public void Reset()
        {
            uncounted.SetValue(0L);
            rate.Set(0.0);
        }
    }
}
