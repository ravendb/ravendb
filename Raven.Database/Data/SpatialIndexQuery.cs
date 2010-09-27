using System;
using Raven.Database.Indexing;

namespace Raven.Database.Data
{
	/// <summary>
	/// A query using spatial filtering
	/// </summary>
	public class SpatialIndexQuery : IndexQuery
	{
		/// <summary>
		/// Gets or sets the latitude.
		/// </summary>
		/// <value>The latitude.</value>
		public double Latitude { get; set; }
		/// <summary>
		/// Gets or sets the longitude.
		/// </summary>
		/// <value>The longitude.</value>
		public double Longitude { get; set; }
		/// <summary>
		/// Gets or sets the radius.
		/// </summary>
		/// <value>The radius.</value>
		public double Radius { get; set; }
		/// <summary>
		/// Gets or sets a value indicating whether [sort by distance].
		/// </summary>
		/// <value><c>true</c> if [sort by distance]; otherwise, <c>false</c>.</value>
		public bool SortByDistance { get; set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="SpatialIndexQuery"/> class.
		/// </summary>
		public SpatialIndexQuery()
		{
			
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="SpatialIndexQuery"/> class.
		/// </summary>
		/// <param name="query">The query.</param>
		public SpatialIndexQuery(IndexQuery query)
		{
			Query = query.Query;
			Start = query.Start;
			Cutoff = query.Cutoff;
			PageSize = query.PageSize;
			FieldsToFetch = query.FieldsToFetch;
			SortedFields = query.SortedFields;
		}

		/// <summary>
		/// Gets the custom query string variables.
		/// </summary>
		/// <returns></returns>
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

		internal override Lucene.Net.Search.Sort GetSort(Lucene.Net.Search.Filter filter, IndexDefinition indexDefinition)
		{
			if (SortByDistance == false)
				return base.GetSort(filter, indexDefinition);

			var dsort = new Lucene.Net.Spatial.Tier.DistanceFieldComparatorSource((Lucene.Net.Spatial.Tier.DistanceFilter)filter);

			return new Lucene.Net.Search.Sort(new Lucene.Net.Search.SortField("foo", dsort, false));
		}
#endif
	}
}