using System;
using Raven.Database.Indexing;

namespace Raven.Database.Data
{
	public class SpatialIndexQuery : IndexQuery
	{
		public double Latitude { get; set; }
		public double Longitude { get; set; }
		public double Radius { get; set; }
		public bool SortByDistance { get; set; }

		public SpatialIndexQuery()
		{
			
		}

		public SpatialIndexQuery(IndexQuery query)
		{
			Query = query.Query;
			Start = query.Start;
			Cutoff = query.Cutoff;
			PageSize = query.PageSize;
			FieldsToFetch = query.FieldsToFetch;
			SortedFields = query.SortedFields;
		}

		protected override string GetCustomQueryStringVariables()
		{
			return string.Format("latitude={0}&longitude={1}&radius={2}&sortByDistance={3}",
				Uri.EscapeDataString(Latitude.ToString()),
				Uri.EscapeDataString(Longitude.ToString()),
				Uri.EscapeDataString(Radius.ToString()),
				Uri.EscapeDataString(SortByDistance ? "true" : "false"));
		}

#if !CLIENT
		internal override Lucene.Net.Search.Filter GetFilter()
		{
			var dq = new Lucene.Net.Spatial.Tier.DistanceQueryBuilder(
					Latitude, Longitude, Radius,
					SpatialIndex.LatField, 
					SpatialIndex.LngField, 
					Lucene.Net.Spatial.Tier.Projectors.CartesianTierPlotter.DefaltFieldPrefix, 
					true);

			return dq.Filter;
		}

		internal override Lucene.Net.Search.Sort GetSort(IndexDefinition indexDefinition)
		{
			if (SortByDistance == false)
				return base.GetSort(indexDefinition);

			var dq = new Lucene.Net.Spatial.Tier.DistanceQueryBuilder(
					Latitude, Longitude, Radius,
					SpatialIndex.LatField,
					SpatialIndex.LngField,
					Lucene.Net.Spatial.Tier.Projectors.CartesianTierPlotter.DefaltFieldPrefix,
					true);
			var dsort = new Lucene.Net.Spatial.Tier.DistanceFieldComparatorSource(dq.DistanceFilter);
			return new Lucene.Net.Search.Sort(new Lucene.Net.Search.SortField("foo", dsort, false));
		}
#endif
	}
}