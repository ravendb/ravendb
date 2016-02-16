using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using metrics.Stats;
using metrics.Support;

namespace metrics.Core
{
	/// <summary>
	/// A metric which calculates the distribution of a value
	/// <see href="http://www.johndcook.com/standard_deviation.html">Accurately computing running variance</see>
	/// </summary>
	public class HistogramMetric : IMetric
	{
		public enum SampleType
		{
			/// <summary>
			/// Uses a uniform sample of 1028 elements, which offers a 99.9%
			/// confidence level with a 5% margin of error assuming a normal
			/// distribution.
			/// </summary>
			Uniform,
			/// <summary>
			/// Uses an exponentially decaying sample of 1028 elements, which offers
			/// a 99.9% confidence level with a 5% margin of error assuming a normal
			/// distribution, and an alpha factor of 0.015, which heavily biases
			/// the sample to the past 5 minutes of measurements.
			/// </summary>
			Biased
		}
		
		private readonly ISample _sample;
		private readonly AtomicLong _min = new AtomicLong();
		private readonly AtomicLong _max = new AtomicLong();
		private readonly AtomicLong _sum = new AtomicLong();

		// These are for the Welford algorithm for calculating 
		// running variance without floating-point doom
		private readonly AtomicLong _varianceM = new AtomicLong();
		private readonly AtomicLong _varianceS = new AtomicLong();
		private readonly AtomicLong _count = new AtomicLong();

		/// <summary>
		/// Creates a new <see cref="HistogramMetric" /> with the given sample type
		/// </summary>
		public HistogramMetric(SampleType type) : this(type.NewSample())
		{
		   
		}

		/// <summary>
		/// Creates a new <see cref="HistogramMetric" /> with the given sample
		/// </summary>
		public HistogramMetric(ISample sample) : this(sample, true)
		{
			_sample = sample;
			Clear();
		}

		private HistogramMetric(ISample sample, bool clear)
		{
			_sample = sample;
			if(clear)
			{
				Clear();
			}
		}

		/// <summary>
		/// Clears all recorded values
		/// </summary>
		public void Clear()
		{
			_sample.Clear();
			_count.Set(0);
			_max.Set(long.MinValue);
			_min.Set(long.MaxValue);
			_sum.Set(0);
			_varianceM.Set(-1);
			_varianceS.Set(0);
		}

		/// <summary>
		/// Adds a recorded value
		/// </summary>
		public void Update(int value)
		{
			Update((long)value);
		}

		/// <summary>
		/// Adds a recorded value
		/// </summary>
		public void Update(long value)
		{
			_count.IncrementAndGet();
			_sample.Update(value);
			SetMax(value);
			SetMin(value);
			_sum.GetAndAdd(value);
			UpdateVariance(value);
		}

		/// <summary>
		/// Returns the number of values recorded
		/// </summary>
		public long Count { get { return _count.Get(); }}

		/// <summary>
		/// Returns the largest recorded value
		/// </summary>
		public double Max
		{
			get
			{
				return Count > 0 ? _max.Get() : 0.0;
			}
		}
		
		/// <summary>
		/// Returns the smallest recorded value
		/// </summary>
		public double Min
		{
			get
			{
				return Count > 0 ? _min.Get() : 0.0;
			}
		}
		
		/// <summary>
		/// Returns the arithmetic mean of all recorded values
		/// </summary>
		public double Mean
		{
			get { return Count > 0 ? _sum.Get()/(double) Count : 0.0; }
		}
		
		/// <summary>
		/// Returns the standard deviation of all recorded values
		/// </summary>
		public double StdDev
		{
			get { return Count > 0 ? Math.Sqrt(Variance) : 0.0; }
		}

		/// <summary>
		/// Returns an array of values at the given percentiles
		/// </summary>
		public double[] Percentiles(params double[] percentiles)
		{
			var scores = new double[percentiles.Length];
			for (var i = 0; i < scores.Length; i++)
			{
				scores[i] = 0.0;
			}

			if (Count > 0)
			{
				var values = _sample.Values.OrderBy(v => v).ToList();

				for (var i = 0; i < percentiles.Length; i++)
				{
					var p = percentiles[i];
					var pos = p*(values.Count + 1);
					if (pos < 1)
					{
						scores[i] = values[0];
					}
					else if (pos >= values.Count)
					{
						scores[i] = values[values.Count - 1];
					}
					else
					{
						var lower = values[(int) pos - 1];
						var upper = values[(int) pos];
						scores[i] = lower + (pos - Math.Floor(pos))*(upper - lower);
					}
				}
			}

			return scores;
		}

		/// <summary>
		/// Returns a list of all values in the histogram's sample
		/// </summary>
		public ICollection<long> Values
		{
			get { return _sample.Values; }
		}

		private double Variance
		{
			get { return Count <= 1 ? 0.0 : BitConverter.Int64BitsToDouble(_varianceS.Get())/(Count - 1); }
		}

		private void SetMax(long potentialMax)
		{
			var done = false;
			while (!done)
			{
				var currentMax = _max.Get();
				done = currentMax >= potentialMax || _max.CompareAndSet(currentMax, potentialMax);
			}
		}

		private void SetMin(long potentialMin)
		{
			var done = false;
			while (!done)
			{
				var currentMin = _min.Get();
				done = currentMin <= potentialMin || _min.CompareAndSet(currentMin, potentialMin);
			}
		}

		private void UpdateVariance(long value)
		{
			// Initialize varianceM to the first reading if it's still blank
			if (_varianceM.CompareAndSet(-1, BitConverter.DoubleToInt64Bits(value)))
			{
				return;
			}

			var done = false;
			while (!done)
			{
				var oldMCas = _varianceM.Get();
				var oldM = BitConverter.Int64BitsToDouble(oldMCas);
				var newM = oldM + ((value - oldM)/Count);
				var oldSCas = _varianceS.Get();
				var oldS = BitConverter.Int64BitsToDouble(oldSCas);
				var newS = oldS + ((value - oldM)*(value - newM));

				done = _varianceM.CompareAndSet(oldMCas, BitConverter.DoubleToInt64Bits(newM)) &&
					   _varianceS.CompareAndSet(oldSCas, BitConverter.DoubleToInt64Bits(newS));
			}
		}

		[IgnoreDataMember]
		public IMetric Copy
		{
			get
			{ 
				var copy = new HistogramMetric(_sample, false /* clear */);
				copy._max.Set(_max);
				copy._min.Set(_min);
				copy._sum.Set(_sum);
				copy._varianceM.Set(_varianceM);
				copy._varianceS.Set(_varianceS);
				copy._count.Set(_count);
				return copy;
			}
		}

	    public void LogJson(StringBuilder sb)
	    {
            

            sb.Append("{\"count\":").Append(Count)
                .Append(",\"max\":").Append(Max)
                .Append(",\"min\":").Append(Min)
                .Append(",\"mean\":").Append(Mean)
                .Append(",\"stdev\":").Append(StdDev)
                .Append(",\"variance\":").Append(Variance).Append("}");
              

        }
        public void LogJson(StringBuilder sb, double[] perc)
        {
	        var percSb= new StringBuilder();
            foreach (var percentile in Percentiles(perc))
	        {
	            percSb.Append(" ").Append(percentile);
	        }
	       
            sb.Append("{\"count\":").Append(Count)
                .Append(",\"max\":").Append(Max)
                .Append(",\"min\":").Append(Min)
                .Append(",\"mean\":").Append(Mean)
                .Append(",\"stdev\":").Append(StdDev)
                //  .Append(",\"variance\":").Append(Variance).Append("}");
                .Append(",\"percentiles\":").Append(percSb).Append("}"); 
     
	    }

	    public double SampleMean
	    {
	        get { return _sample.Count == 0 ? 0 : _sample.Values.Average(); }
	    }

	    public int SampleCount
	    {
            get { return _sample.Count; }
	    }

	    public long SampleMin
	    {
            get { return _sample.Count == 0 ? 0 : _sample.Values.Min(); }
	    }

	    public double SampleMax
	    {
            get { return _sample.Count == 0 ? 0 : _sample.Values.Max(); }
	    }
	}

    public static class SampleExtensions
	{
		public static ISample NewSample(this HistogramMetric.SampleType type)
		{
			switch (type)
			{
				case HistogramMetric.SampleType.Uniform:
					return new UniformSample(1028);
				case HistogramMetric.SampleType.Biased:
					return new ExponentiallyDecayingSample(1028, 0.015);
				default:
					throw new ArgumentOutOfRangeException("type");
			}
		}
	}
}
