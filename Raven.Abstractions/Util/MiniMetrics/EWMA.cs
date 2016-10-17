using System;
using System.Threading;

namespace Raven.Abstractions.Util.MiniMetrics
{
    /// <summary>
    ///     An exponentially-weighted moving average.
    ///     <a href="http://www.teamquest.com/pdfs/whitepaper/ldavg1.pdf">UNIX Load Average Part 1: How It Works</a>
    ///     <a href="http://www.teamquest.com/pdfs/whitepaper/ldavg2.pdf">UNIX Load Average Part 2: Not Your Average Average</a>
    ///     <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">EMA</a>
    /// </summary>
    public class Ewma
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
        private readonly double _alpha;
        private readonly double _interval;


        private volatile bool _initialized;
        private double _rate;

        private long _uncounted;

        public Ewma(double alpha, long interval)
        {
            _interval = interval * Clock.NanosecondsInSecond;
            _alpha = alpha;
        }

        public static Ewma OneSecondEwma()
        {
            return new Ewma(M1Second, 1);
        }

        public static Ewma OneMinuteEwma()
        {
            return new Ewma(M1Alpha, Interval);
        }

        public static Ewma FiveMinuteEwma()
        {
            return new Ewma(M5Alpha, Interval);
        }

        public static Ewma FifteenMinuteEwma()
        {
            return new Ewma(M15Alpha, Interval);
        }

        public void Update(long value)
        {
            Interlocked.Add(ref _uncounted, value);
        }

        public void Tick()
        {
            var count = Interlocked.Exchange(ref _uncounted, 0);

            var instantRate = count / _interval;
            if (_initialized)
            {
                double doubleRate = Volatile.Read(ref _rate);
                Volatile.Write(ref _rate, doubleRate + _alpha * (instantRate - doubleRate));
            }
            else
            {
                Volatile.Write(ref _rate, instantRate);
                _initialized = true;
            }
        }

        public double GetRate()
        {
            return Volatile.Read(ref _rate) * Clock.NanosecondsInSecond;
        }
    }
}