using System;

namespace Raven.NewClient.Client.Metrics
{
    /// <summary>
    ///  An exponentially-weighted moving average
    /// </summary>
    /// <see href="http://www.teamquest.com/pdfs/whitepaper/ldavg1.pdf"/>
    /// <see href="http://www.teamquest.com/pdfs/whitepaper/ldavg2.pdf" />
    internal class EWMA
    {
        private static readonly double M1Second = 1 - Math.Exp(-1);
        public static readonly double M1Alpha = 1 - Math.Exp(-5 / 60.0);
        private static readonly double M5Alpha = 1 - Math.Exp(-5 / 60.0 / 5);
        private static readonly double M15Alpha = 1 - Math.Exp(-5 / 60.0 / 15);

        private readonly AtomicLong _uncounted = new AtomicLong(0);
        private readonly double _alpha;
        private readonly double _interval;
        private volatile bool _initialized;
        private VolatileDouble _rate;

        /// <summary>
        /// Creates a new EWMA which is equivalent to one second load average and which expects to be ticked every 1 seconds.
        /// </summary>
        public static EWMA OneSecondEWMA()
        {
            return new EWMA(M1Second, 1, TimeUnit.Seconds);
        }

        /// <summary>
        /// Creates a new EWMA which is equivalent to the UNIX one minute load average and which expects to be ticked every 5 seconds.
        /// </summary>
        public static EWMA OneMinuteEWMA()
        {
            return new EWMA(M1Alpha, 5, TimeUnit.Seconds);
        }

        /// <summary>
        /// Creates a new EWMA which is equivalent to the UNIX five minute load average and which expects to be ticked every 5 seconds.
        /// </summary>
        /// <returns></returns>
        public static EWMA FiveMinuteEWMA()
        {
            return new EWMA(M5Alpha, 5, TimeUnit.Seconds);
        }

        /// <summary>
        ///  Creates a new EWMA which is equivalent to the UNIX fifteen minute load average and which expects to be ticked every 5 seconds.
        /// </summary>
        /// <returns></returns>
        public static EWMA FifteenMinuteEWMA()
        {
            return new EWMA(M15Alpha, 5, TimeUnit.Seconds);
        }

        /// <summary>
        /// Create a new EWMA with a specific smoothing constant.
        /// </summary>
        /// <param name="alpha">The smoothing constant</param>
        /// <param name="interval">The expected tick interval</param>
        /// <param name="intervalUnit">The time unit of the tick interval</param>
        public EWMA(double alpha, long interval, TimeUnit intervalUnit)
        {
            _interval = intervalUnit.ToNanos(interval);
            _alpha = alpha;
        }

        /// <summary>
        ///  Update the moving average with a new value.
        /// </summary>
        /// <param name="n"></param>
        public void Update(long n)
        {
            _uncounted.AddAndGet(n);
        }

        /// <summary>
        /// Mark the passage of time and decay the current rate accordingly.
        /// </summary>
        public void Tick()
        {
            var count = _uncounted.GetAndSet(0);
            var instantRate = count / _interval;
            if (_initialized)
            {
                _rate += _alpha * (instantRate - _rate);
            }
            else
            {
                _rate.Set(instantRate);
                _initialized = true;
            }
        }

        /// <summary>
        /// Returns the rate in the given units of time.
        /// </summary>
        public double Rate(TimeUnit rateUnit)
        {
            var nanos = rateUnit.ToNanos(1);
            return _rate * nanos;
        }
    }
}
