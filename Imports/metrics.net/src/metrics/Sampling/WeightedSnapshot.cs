
using System;
using System.Collections.Generic;
using System.Linq;
namespace Metrics.Sampling
{
    public sealed class WeightedSample
    {
        public readonly long Value;
        public readonly double Weight;

        public WeightedSample(long value, double weight)
        {
            this.Value = value;
            this.Weight = weight;
        }
    }

    public sealed class WeightedSnapshot : Snapshot
    {
        private readonly long count;
        private readonly long[] values;
        private readonly double[] normWeights;
        private readonly double[] quantiles;

        private class WeightedSampleComparer : IComparer<WeightedSample>
        {
            public static readonly IComparer<WeightedSample> Instance = new WeightedSampleComparer();

            public int Compare(WeightedSample x, WeightedSample y)
            {
                return Comparer<long>.Default.Compare(x.Value, y.Value);
            }
        }

        public WeightedSnapshot(long count, IEnumerable<WeightedSample> values)
        {
            this.count = count;
            var sample = values.ToArray();
            Array.Sort(sample, WeightedSampleComparer.Instance);

            var sumWeight = sample.Sum(s => s.Weight);

            this.values = new long[sample.Length];
            this.normWeights = new double[sample.Length];
            this.quantiles = new double[sample.Length];

            for (int i = 0; i < sample.Length; i++)
            {
                this.values[i] = sample[i].Value;
                this.normWeights[i] = sample[i].Weight / sumWeight;
                if (i > 0)
                {
                    this.quantiles[i] = this.quantiles[i - 1] + this.normWeights[i - 1];
                }
            }
        }

        public long Count { get { return this.count; } }
        public int Size { get { return this.values.Length; } }

        public long Max { get { return this.values.LastOrDefault(); } }
        public long Min { get { return this.values.FirstOrDefault(); } }

        public double Mean
        {
            get
            {
                if (this.values.Length == 0)
                {
                    return 0.0;
                }

                double sum = 0;
                for (int i = 0; i < this.values.Length; i++)
                {
                    sum += this.values[i] * this.normWeights[i];
                }
                return sum;
            }
        }

        public double StdDev
        {
            get
            {
                if (this.Size <= 1)
                {
                    return 0;
                }

                double mean = this.Mean;
                double variance = 0;

                for (int i = 0; i < this.values.Length; i++)
                {
                    double diff = values[i] - mean;
                    variance += this.normWeights[i] * diff * diff;
                }

                return Math.Sqrt(variance);
            }
        }

        public double Median { get { return GetValue(0.5d); } }
        public double Percentile75 { get { return GetValue(0.75d); } }
        public double Percentile95 { get { return GetValue(0.95d); } }
        public double Percentile98 { get { return GetValue(0.98d); } }
        public double Percentile99 { get { return GetValue(0.99d); } }
        public double Percentile999 { get { return GetValue(0.999d); } }

        public IEnumerable<long> Values { get { return this.values.AsEnumerable(); } }

        public double GetValue(double quantile)
        {
            if (quantile < 0.0 || quantile > 1.0 || double.IsNaN(quantile))
            {
                throw new ArgumentException(string.Format("{0} is not in [0..1]", quantile));
            }

            if (this.Size == 0)
            {
                return 0;
            }

            int posx = Array.BinarySearch(this.quantiles, quantile);
            if (posx < 0)
            {
                posx = ~posx - 1;
            }

            if (posx < 1)
            {
                return this.values[0];
            }

            if (posx >= this.values.Length)
            {
                return values[values.Length - 1];
            }

            return values[posx];
        }
    }
}
