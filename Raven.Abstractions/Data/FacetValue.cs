using System;

namespace Raven.Abstractions.Data
{
	public class FacetValue
	{
		public string Range { get; set; }

        public int Hits { get; set; }

	    public double? Count { get; set; }
        public double? Sum { get; set; }
        public double? Max { get; set; }
        public double? Min { get; set; }
        public double? Average { get; set; }

        public double? GetAggregation(FacetAggregation aggregation)
        {
            switch (aggregation)
            {
                case FacetAggregation.None:
                    return null;
                case FacetAggregation.Count:
                    return Count;
                case FacetAggregation.Max:
                    return Max;
                case FacetAggregation.Min:
                    return Min;
                case FacetAggregation.Average:
                    return Average;
                case FacetAggregation.Sum:
                    return Sum;
                default:
                    return null;
            }
        }

		public override string ToString()
		{
			var msg = Range + " -  Hits: " + Hits + ",";
			if (Count != null)
				msg += "Count: " + Count + ",";
			if(Sum != null)
				msg += "Sum: " + Sum + ",";
			if (Max != null)
				msg += "Max: " + Max + ",";
			if (Min != null)
				msg += "Min: " + Min + ",";
			if (Average != null)
				msg += "Average: " + Average + ",";

			msg = msg.TrimEnd(',', ' ');
			return msg;
		}
	}
}
