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
			var msg = "";
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
