using System.Linq;
using Raven.Client.Client;
using Raven.Database.Data;

namespace Raven.Client.Document
{
	public class SpatialDocumentQuery<T> : DocumentQuery<T>
	{
		protected double lat, lng, radius;
		
		public SpatialDocumentQuery(DocumentSession session, IDatabaseCommands databaseCommands, string indexName, string[] projectionFields)
			: base(session, databaseCommands, indexName, projectionFields)
		{
		}

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