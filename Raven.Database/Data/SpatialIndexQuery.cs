using System;

namespace Raven.Database.Data
{
	public class SpatialIndexQuery : IndexQuery
	{
		public double Latitude { get; set; }
		public double Longitude { get; set; }
		public double Radius { get; set; }
		public bool SortByDistance { get; set; }

		protected override string GetCustomQueryStringVariables()
		{
			return string.Format("_spatial=true&_lat={0}&_lng={1}&_radius={2}&_sortByDistance={3}",
				Uri.EscapeDataString(Latitude.ToString()),
				Uri.EscapeDataString(Longitude.ToString()),
				Uri.EscapeDataString(Radius.ToString()),
				Uri.EscapeDataString(SortByDistance ? "true" : "false"));
		}
	}
}