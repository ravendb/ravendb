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

	    public override string ToString()
	    {
	        return string.Format("Range: {0}, Hits: {1}, Count: {2}, Sum: {3}, Max: {4}, Min: {5}, Average: {6}", Range, Hits, Count, Sum, Max, Min, Average);
	    }
	}
}
