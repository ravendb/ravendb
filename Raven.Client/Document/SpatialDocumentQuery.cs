using System.Linq;
using Raven.Client.Client;
using Raven.Database.Data;

namespace Raven.Client.Document
{
	/// <summary>
	/// A spatial query allows to perform spatial filtering on the index
	/// </summary>
	public class SpatialDocumentQuery<T> : DocumentQuery<T>
	{
		private double lat, lng, radius;

		/// <summary>
		/// Initializes a new instance of the <see cref="SpatialDocumentQuery&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="session">The session.</param>
		/// <param name="databaseCommands">The database commands.</param>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="projectionFields">The projection fields.</param>
		public SpatialDocumentQuery(DocumentSession session, IDatabaseCommands databaseCommands, string indexName, string[] projectionFields)
			: base(session, databaseCommands, indexName, projectionFields)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="SpatialDocumentQuery&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="documentQuery">The document query.</param>
		/// <param name="radius">The radius.</param>
		/// <param name="latitude">The latitude.</param>
		/// <param name="longitude">The longitude.</param>
		public SpatialDocumentQuery(DocumentQuery<T> documentQuery, double radius, double latitude, double longitude)
			: base(documentQuery)
		{
			this.radius = radius;
			lat = latitude;
			lng = longitude;
		}

		protected override IndexQuery GenerateIndexQuery(string query)
		{
			return new SpatialIndexQuery
			{
				Query = query,
				PageSize = pageSize,
				Start = start,
				Cutoff = cutoff,
				SortedFields = orderByFields.Select(x => new SortedField(x)).ToArray(),
				FieldsToFetch = projectionFields,
				Latitude = lat,
				Longitude = lng,
				Radius = radius
			};
		}
	}
}