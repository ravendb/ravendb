#if !NET35

using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Listeners;

namespace Raven.Client.Document
{
	/// <summary>
	/// A spatial query allows to perform spatial filtering on the index
	/// </summary>
	public class AsyncSpatialDocumentQuery<T> : AsyncDocumentQuery<T>
	{
		private readonly double lat, lng, radius;

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncSpatialDocumentQuery{T}"/> class.
		/// </summary>
		public AsyncSpatialDocumentQuery(
			InMemoryDocumentSessionOperations session,
#if !SILVERLIGHT
			IDatabaseCommands databaseCommands, 
#endif
#if !NET35
			IAsyncDatabaseCommands asyncDatabaseCommands,
#endif
			string indexName, 
			string[] projectionFields,
			IDocumentQueryListener[] queryListeners)
			: base(session, 
#if !SILVERLIGHT
				   databaseCommands, 
#endif
#if !NET35
				   asyncDatabaseCommands,
#endif
				   indexName, 
				   projectionFields,
				   queryListeners)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncSpatialDocumentQuery{T}"/> class.
		/// </summary>
		/// <param name="documentQuery">The document query.</param>
		/// <param name="radius">The radius.</param>
		/// <param name="latitude">The latitude.</param>
		/// <param name="longitude">The longitude.</param>
		public AsyncSpatialDocumentQuery(AsyncDocumentQuery<T> documentQuery, double radius, double latitude, double longitude)
			: base(documentQuery)
		{
			this.radius = radius;
			lat = latitude;
			lng = longitude;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncSpatialDocumentQuery{T}"/> class.
		/// </summary>
		/// <param name="documentQuery">The document query.</param>
		public AsyncSpatialDocumentQuery(AsyncDocumentQuery<T> documentQuery)
			: base(documentQuery)
		{
			var other = documentQuery as AsyncSpatialDocumentQuery<T>;
			if (other == null)
				return;

			radius = other.radius;
			lat = other.lat;
			lng = other.lng;				
		}


		
		/// <summary>
		/// Generates the index query.
		/// </summary>
		/// <param name="query">The query.</param>
		/// <returns></returns>
		protected override IndexQuery GenerateIndexQuery(string query)
		{
			var generateIndexQuery = new SpatialIndexQuery
			{
				Query = query,
				Start = start,
				Cutoff = cutoff,
				SortedFields = orderByFields.Select(x => new SortedField(x)).ToArray(),
				FieldsToFetch = projectionFields,
				Latitude = lat,
				Longitude = lng,
				Radius = radius,
			};
			if (pageSize != null)
				generateIndexQuery.PageSize = pageSize.Value;

			return generateIndexQuery;
		}

		public override IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields)
		{
			return new AsyncSpatialDocumentQuery<TProjection>((AsyncDocumentQuery<TProjection>)base.SelectFields<TProjection>(fields), radius, lat, lng);
		}
	}
}
#endif