using System;

namespace Raven.Abstractions.Data
{
	public class FacetValue
	{
		public string Range { get; set; }

        public int Hits { get; set; }

	    public double? Value { get; set; }

		public override string ToString()
		{ 
			return string.Format("{0}: {1}", Range, Value);
		}
	}
}
