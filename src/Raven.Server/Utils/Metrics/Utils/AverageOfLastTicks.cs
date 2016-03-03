using System.Threading;

namespace Metrics.Utils
{
    /// <summary>
    /// An exponentially-weighted moving average.
    /// <a href="http://www.teamquest.com/pdfs/whitepaper/ldavg1.pdf">UNIX Load Average Part 1: How It Works</a>
    /// <a href="http://www.teamquest.com/pdfs/whitepaper/ldavg2.pdf">UNIX Load Average Part 2: Not Your Average Average</a>
    /// <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">EMA</a>
    /// </summary>
    public class AverageOfLastTicks
    {
        private readonly int _ticksAmount;
        private readonly int _intervalInSeconds;
        private AtomicLong uncounted = new AtomicLong();
        private long[] _countPerTickBuffer;
        readonly object _rateLocker = new object();
        private int _countPerTicksBufferPosition = 0;
        

        public AverageOfLastTicks(int ticksAmount, int intervalInSeconds=1)
        {
            _ticksAmount = ticksAmount;
            _intervalInSeconds = intervalInSeconds;
            this._intervalInSeconds = intervalInSeconds;
            this._countPerTickBuffer = new long[this._ticksAmount];
            
        }

        public void Update(long value)
        {
            uncounted.Add(value); 
        }

        public void Tick()
        {
            long count = uncounted.GetAndReset();
            var countAverage = count/_intervalInSeconds;
            Volatile.Write(ref _countPerTicksBufferPosition, (_countPerTicksBufferPosition + 1) % this._ticksAmount);
            _countPerTickBuffer[_countPerTicksBufferPosition] = countAverage;
            
        }

        
        public double GetRate()
        {
            try
            {
                if (Monitor.TryEnter(_rateLocker, 1000) == false)
                    return -1;
                double sum=0;
                for (var i = 0; i < _ticksAmount; i++)
                {
                    sum += _countPerTickBuffer[i];
                }

                return sum/ _ticksAmount;
            }
            finally
            {
                Monitor.Exit(_rateLocker);
            }
            
        }

        public void Reset()
        {
            uncounted.SetValue(0L);
            for (var i = 0; i < _ticksAmount; i++)
            {
                _countPerTickBuffer[i]=0;
            }
        }
    }
}
