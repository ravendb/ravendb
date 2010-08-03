using System;

namespace Raven.Database.Data
{
	public class SpatialIndexQuery : IndexQuery
	{
		public double Latitude { get; set; }
		public double Longitude { get; set; }
		public double Miles { get; set; }
		public bool SortByDistance { get; set; }

		protected override string GetCustomQueryStringVariables()
		{
			return string.Format("&lat={0}&lng={1}&miles={2}&sortByDistance={3}",
				Uri.EscapeDataString(Latitude.ToString()),
				Uri.EscapeDataString(Longitude.ToString()),
				Uri.EscapeDataString(Miles.ToString()),
				Uri.EscapeDataString(SortByDistance ? "true" : "false"));
		}
	}
}