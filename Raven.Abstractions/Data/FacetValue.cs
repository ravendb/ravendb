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

		
	}
}
