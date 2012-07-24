namespace Raven.Abstractions.Data
{
	public static class Constants
	{
		public const string RavenShardId = "Raven-Shard-Id";
		public const string RavenAuthenticatedUser = "Raven-Authenticated-User";
		public const string LastModified = "Last-Modified";
		public const string SystemDatabase = "<system>";
		public const string TemporaryScoreValue = "Temp-Index-Score";
		public const string SpatialFieldName = "__spatial";
		public const string SpatialShapeFieldName = "__spatialShape";
		public const string DistanceFieldName = "__distance";
		public const string RandomFieldName = "__random";
		public const string NullValueNotAnalyzed = "[[NULL_VALUE]]";
		public const string EmptyStringNotAnalyzed = "[[EMPTY_STRING]]";
		public const string NullValue = "NULL_VALUE";
		public const string EmptyString = "EMPTY_STRING";
		public const string DocumentIdFieldName = "__document_id";
		public const string ReduceKeyFieldName = "__reduce_key";
		public const string IntersectSeperator = " INTERSECT ";
		public const string RavenClrType = "Raven-Clr-Type";
		public const string RavenEntityName = "Raven-Entity-Name";
		public const string RavenReadOnly = "Raven-Read-Only";
		public const string AllFields = "__all_fields";
		// This is used to indicate that a document exists in an uncommitted transaction
		public const string RavenDocumentDoesNotExists = "Raven-Document-Does-Not-Exists";
		public const string Metadata = "@metadata";
		public const string NotForReplication = "Raven-Not-For-Replication";
		public const string RavenDeleteMarker = "Raven-Delete-Marker";
	}
}