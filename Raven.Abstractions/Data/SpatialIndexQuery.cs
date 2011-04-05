//-----------------------------------------------------------------------
// <copyright file="SpatialIndexQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
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
				Uri.EscapeDataString(Radius.ToString()));
		}
	}
}
