namespace Raven.NewClient.Client.Data
{
    public class FacetValue
    {
        /// <summary>
        /// Name of range for which facet value applies.
        /// </summary>
        public string Range { get; set; }

        /// <summary>
        /// Number of terms that are covered by this facet.
        /// </summary>
        public int Hits { get; set; }

        /// <summary>
        /// Stores count value if FacetAggregation.Count was set.
        /// </summary>
        public int? Count { get; set; }

        /// <summary>
        /// Stores sum of all values if FacetAggregation.Sum was set.
        /// </summary>
        public double? Sum { get; set; }

        /// <summary>
        /// Stores maximum value if FacetAggregation.Max was set.
        /// </summary>
        public double? Max { get; set; }

        /// <summary>
        /// Stores minimum value if FacetAggregation.Min was set.
        /// </summary>
        public double? Min { get; set; }

        /// <summary>
        /// Stores average value if FacetAggregation.Average was set.
        /// </summary>
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
            if (Sum != null)
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