namespace Raven.Client.Documents.Queries.Facets
{
    public class FacetValue
    {
        /// <summary>
        /// Name of range for which facet value applies.
        /// </summary>
        public string Range { get; set; }

        /// <summary>
        /// Stores count value.
        /// </summary>
        public int Count { get; set; }

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

        public override string ToString()
        {
            var msg = Range + " -  Count: " + Count + ",";
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
