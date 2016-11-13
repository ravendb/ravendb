using System;

namespace Raven.Abstractions.Counters
{
    public class CounterSummary
    {
        public string GroupName { get; set; }

        public string CounterName { get; set; }

        public long Total
        {
            get { return Math.Abs(Increments) - Math.Abs(Decrements); }
        }
        public long Increments { get; set; }

        public long Decrements { get; set; }
    }
}
