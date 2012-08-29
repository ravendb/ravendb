using System;
using System.Security.Cryptography;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
	public static class Constants
	{
		
		public const string RavenShardId = "Raven-Shard-Id";
		public const string RavenAuthenticatedUser = "Raven-Authenticated-User";
		public const string LastModified = "Last-Modified";
		public const string SystemDatabase = "<system>";
		public const string TemporaryScoreValue = "Temp-Index-Score";
		public const string DefaultSpatialFieldName = "__spatial";
		public const string SpatialShapeFieldName = "__spatialShape";
		public const double DefaultSpatialDistanceErrorPct = 0.025d;
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
		public const string ActiveBundles = "Raven/ActiveBundles";

		//Paths
		public const string RavenDataDir = "Raven/DataDir";
		public const string RavenLogsPath = "Raven/Esent/LogsPath";
		public const string RavenIndexPath = "Raven/IndexStoragePath";

		//Encryption
		public const string DontEncryptDocumentsStartingWith = "Raven/";
		public const string AlgorithmTypeSetting = "Raven/Encryption/Algorithm";
		public const string EncryptionKeySetting = "Raven/Encryption/Key";
		public const string EncryptIndexes = "Raven/Encryption/EncryptIndexes";

		public const string InDatabaseKeyVerificationDocumentName = "Raven/Encryption/Verification";
		public static readonly RavenJObject InDatabaseKeyVerificationDocumentContents = new RavenJObject {
			{ "Text", "The encryption is correct." }
		};

		public const int DefaultGeneratedEncryptionKeyLength = 256 / 8;
		public const int MinimumAcceptableEncryptionKeyLength = 64 / 8;

		public const int DefaultKeySizeToUseInActualEncryptionInBits = 128;
		public const int Rfc2898Iterations = 1000;

		public const int DefaultIndexFileBlockSize = 12 * 1024;

		public static readonly Type DefaultCryptoServiceProvider = typeof(AesManaged);

		//Quotas
		public const string DocsHardLimit = "Raven/Quotas/Documents/HardLimit";
		public const string DocsSoftLimit = "Raven/Quotas/Documents/SoftLimit";
		public const string SizeHardLimitInKB = "Raven/Quotas/Size/HardLimitInKB";
		public const string SizeSoftLimitInKB = "Raven/Quotas/Size/SoftMarginInKB";

		//Replications
		public const string RavenReplicationSource = "Raven-Replication-Source";
		public const string RavenReplicationVersion = "Raven-Replication-Version";
		public const string RavenReplicationHistory = "Raven-Replication-History";
		public const string RavenReplicationVersionHiLo = "Raven/Replication/VersionHilo";
		public const string RavenReplicationConflict = "Raven-Replication-Conflict";
		public const string RavenReplicationConflictDocument = "Raven-Replication-Conflict-Document";
		public const string RavenReplicationSourcesBasePath = "Raven/Replication/Sources";
		public const string RavenReplicationDestinations = "Raven/Replication/Destinations";
		public const string RavenReplicationDestinationsBasePath = "Raven/Replication/Destinations/";

		public const string RavenReplicationDocsTombstones = "Raven/Replication/Docs/Tombstones";
		public const string RavenReplicationAttachmentsTombstones = "Raven/Replication/Attachments/Tombstones";

		public const int ChangeHistoryLength = 50;
	}
}