using System;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
    public static partial class Constants
	{
		static Constants()
		{
			InResourceKeyVerificationDocumentContents = new RavenJObject
			{
				{"Text", "The encryption is correct."}
			};
			InResourceKeyVerificationDocumentContents.EnsureCannotBeChangeAndEnableSnapshotting();
		}

		public const string RavenClientPrimaryServerUrl = "Raven-Client-Primary-Server-Url";
		public const string RavenClientPrimaryServerLastCheck = "Raven-Client-Primary-Server-LastCheck";
		public const string RavenForcePrimaryServerCheck = "Raven-Force-Primary-Server-Check";

		public const string RavenShardId = "Raven-Shard-Id";
        // turned into CurrentOperationContext.RavenAuthenticatedUser.Value
        //public const string RavenAuthenticatedUser = "Raven-Authenticated-User";
		public const string LastModified = "Last-Modified";
        public const string CreationDate = "Creation-Date";
        public const string RavenCreationDate = "Raven-Creation-Date";
        public const string RavenLastModified = "Raven-Last-Modified";
		public const string SystemDatabase = "<system>";
		public const string TemporaryScoreValue = "Temp-Index-Score";
		public const string RandomFieldName = "__random";
		public const string CustomSortFieldName = "__customSort";
		public const string NullValueNotAnalyzed = "[[NULL_VALUE]]";
		public const string EmptyStringNotAnalyzed = "[[EMPTY_STRING]]";
		public const string NullValue = "NULL_VALUE";
		public const string EmptyString = "EMPTY_STRING";
		public const string DocumentIdFieldName = "__document_id";
		public const string ReduceKeyFieldName = "__reduce_key";
		public const string ReduceValueFieldName = "__reduced_val";
		public const string IntersectSeparator = " INTERSECT ";
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
		public const string AllowBundlesChange = "Raven-Temp-Allow-Bundles-Change";
		public const string RavenAlerts = "Raven/Alerts";
		public const string RavenJavascriptFunctions = "Raven/Javascript/Functions";

		public const string MemoryLimitForProcessing_BackwardCompatibility = "Raven/MemoryLimitForIndexing";
		public const string MemoryLimitForProcessing = "Raven/MemoryLimitForProcessing";
        public const string RunInMemory = "Raven/RunInMemory";
	    public const string ExposeConfigOverTheWire = "Raven/ExposeConfigOverTheWire";

		// Server
		public const string MaxConcurrentServerRequests = "Raven/MaxConcurrentServerRequests";
		public const string MaxConcurrentMultiGetRequests = "Raven/MaxConcurrentMultiGetRequests";
	    public const string MaxConcurrentRequestsForDatabaseDuringLoad = "Raven/MaxConcurrentRequestsForDatabaseDuringLoad";
        public const string MaxSecondsForTaskToWaitForDatabaseToLoad = "Raven/MaxSecondsForTaskToWaitForDatabaseToLoad";
	    public const string RejectClientsModeEnabled = "Raven/RejectClientsModeEnabled";
	    public const string RavenServerBuild = "Raven-Server-Build";

		// Indexing
		public const string RavenPrefetchingDurationLimit = "Raven/Prefetching/DurationLimit";
		public const int DefaultPrefetchingDurationLimit = 5000;
		public const string BulkImportBatchTimeout = "Raven/BulkImport/BatchTimeout";
		public const int BulkImportDefaultTimeoutInMs = 60000;
	    public const string IndexingDisabled = "Raven/IndexingDisabled";
		public const string MaxNumberOfItemsToProcessInTestIndexes = "Raven/Indexing/MaxNumberOfItemsToProcessInTestIndexes";

		public const string IndexReplacePrefix = "Raven/Indexes/Replace/";

		//Paths
		public const string RavenDataDir = "Raven/DataDir";
		public const string RavenEsentLogsPath = "Raven/Esent/LogsPath";
        public const string RavenTxJournalPath = "Raven/TransactionJournalsPath";
		public const string RavenIndexPath = "Raven/IndexStoragePath";

		//Files
		public const int WindowsMaxPath = 260 - 30;
		public const int LinuxMaxPath = 4096;
		public const int LinuxMaxFileNameLength = WindowsMaxPath;
		public static readonly string[] WindowsReservedFileNames = { "con", "prn", "aux", "nul", "com1", "com2","com3", "com4", "com5", "com6", "com7", "com8", "com9",
																		"lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9", "clock$" };

		//Encryption
		public const string DontEncryptDocumentsStartingWith = "Raven/";
		public const string AlgorithmTypeSetting = "Raven/Encryption/Algorithm";
		public const string EncryptionKeySetting = "Raven/Encryption/Key";
		public const string EncryptionKeyBitsPreferenceSetting = "Raven/Encryption/KeyBitsPreference";
		public const string EncryptIndexes = "Raven/Encryption/EncryptIndexes";

		public const string InResourceKeyVerificationDocumentName = "Raven/Encryption/Verification";
		public static readonly RavenJObject InResourceKeyVerificationDocumentContents;

		public const int DefaultGeneratedEncryptionKeyLength = 256/8;
		public const int MinimumAcceptableEncryptionKeyLength = 64/8;

		public const int DefaultKeySizeToUseInActualEncryptionInBits = 128;
		public const int Rfc2898Iterations = 1000;

		public const int DefaultIndexFileBlockSize = 12*1024;

		public static readonly Type DefaultCryptoServiceProvider = typeof(System.Security.Cryptography.AesCryptoServiceProvider);

		//Quotas
		public const string DocsHardLimit = "Raven/Quotas/Documents/HardLimit";
		public const string DocsSoftLimit = "Raven/Quotas/Documents/SoftLimit";
		public const string SizeHardLimitInKB = "Raven/Quotas/Size/HardLimitInKB";
		public const string SizeSoftLimitInKB = "Raven/Quotas/Size/SoftMarginInKB";

		//Replications
	    public const string RavenIndexAndTransformerReplicationLatencyInSec = "Raven/Replication/IndexAndTransformerReplicationLatency"; //in seconds
	    public const int DefaultRavenIndexAndTransformerReplicationLatencyInSec = 600;
		public const string RavenReplicationSource = "Raven-Replication-Source";
		public const string RavenReplicationVersion = "Raven-Replication-Version";
		public const string RavenReplicationHistory = "Raven-Replication-History";
		public const string RavenReplicationConflict = "Raven-Replication-Conflict";
		public const string RavenReplicationConflictDocument = "Raven-Replication-Conflict-Document";
		public const string RavenReplicationSourcesBasePath = "Raven/Replication/Sources";
		public const string RavenReplicationDestinations = "Raven/Replication/Destinations";
		public const string RavenReplicationDestinationsBasePath = "Raven/Replication/Destinations/";
		public const string RavenReplicationConfig = "Raven/Replication/Config";

		public const string RavenReplicationDocsTombstones = "Raven/Replication/Docs/Tombstones";

        [Obsolete("Use RavenFS instead.")]
		public const string RavenReplicationAttachmentsTombstones = "Raven/Replication/Attachments/Tombstones";

        //Periodic export
		public const string RavenPeriodicExportsDocsTombstones = "Raven/PeriodicExports/Docs/Tombstones";

        [Obsolete("Use RavenFS instead.")]
		public const string RavenPeriodicExportsAttachmentsTombstones = "Raven/PeriodicExports/Attachments/Tombstones";

		public const int ChangeHistoryLength = 50;

		//Spatial
		public const string DefaultSpatialFieldName = "__spatial";
		public const string SpatialShapeFieldName = "__spatialShape";
		public const double DefaultSpatialDistanceErrorPct = 0.025d;
		public const string DistanceFieldName = "__distance";
		/// <summary>
		/// The International Union of Geodesy and Geophysics says the Earth's mean radius in KM is:
		///
		/// [1] http://en.wikipedia.org/wiki/Earth_radius
		/// </summary>
		public const double EarthMeanRadiusKm = 6371.0087714;
		public const double MilesToKm = 1.60934;
		
		//Versioning
		public const string RavenCreateVersion = "Raven-Create-Version";
	    public const string RavenIgnoreVersioning = "Raven-Ignore-Versioning";

		public const string RavenClientVersion = "Raven-Client-Version";
        public const string RavenDefaultQueryTimeout = "Raven_Default_Query_Timeout";
		public const string NextPageStart = "Next-Page-Start";

        /// <summary>
        /// if no encoding information in headers of incoming request, this encoding is assumed
        /// </summary>
        public const string DefaultRequestEncoding = "UTF-8";
        
		public const string DocumentsByEntityNameIndex = "Raven/DocumentsByEntityName";
		
		//Counters
		public const byte GroupSeperator = 29;
		public const char GroupSeperatorChar = (char)GroupSeperator;
		public const string GroupSeperatorString = "\u001D";

        public const string MetadataEtagField = "ETag";

		public const string TempUploadsDirectoryName = "RavenTempUploads";

		public const string DataCouldNotBeDecrypted = "<data could not be decrypted>";

		// Backup

		public const string DatabaseDocumentFilename = "Database.Document";
		public const string FilesystemDocumentFilename = "Filesystem.Document";
	    public const string IncrementalBackupAlertTimeout = "Raven/IncrementalBackup/AlertTimeoutHours";
        public const string IncrementalBackupRecurringAlertTimeout = "Raven/IncrementalBackup/RecurringAlertTimeoutDays";
		public const string IncrementalBackupState = "IncrementalBackupState.Document";


		// General
		public const string RavenDatabasesPrefix = "Raven/Databases/";

        public static partial class FileSystem
        {
			public const string Prefix = "Raven/FileSystems/";
            public const string DataDirectory = "Raven/FileSystem/DataDir";
            public const string IndexStorageDirectory = "Raven/FileSystem/IndexStoragePath";
            public const string MaximumSynchronizationInterval = "Raven/FileSystem/MaximumSynchronizationInterval";
            public const string Storage = "Raven/FileSystem/Storage";

	        public static class Versioning
	        {
				public const string ChangesToRevisionsAllowed = "Raven/FileSystem/Versioning/ChangesToRevisionsAllowed";
	        }
        }

		// Subscriptions
		public const string RavenSubscriptionsPrefix = "Raven/Subscriptions/";
        
        public static partial class Esent
        {
            public const string CircularLog = "Raven/Esent/CircularLog";
            public const string CacheSizeMax = "Raven/Esent/CacheSizeMax";
            public const string MaxVerPages = "Raven/Esent/MaxVerPages";
            public const string PreferredVerPages = "Raven/Esent/PreferredVerPages";
            public const string LogFileSize = "Raven/Esent/LogFileSize";
            public const string LogBuffers = "Raven/Esent/LogBuffers";
            public const string MaxCursors = "Raven/Esent/MaxCursors";
            public const string DbExtensionSize = "Raven/Esent/DbExtensionSize";
        }

        public static partial class Voron
        {
            public const string AllowIncrementalBackups = "Raven/Voron/AllowIncrementalBackups";
            public const string InitialFileSize = "Raven/Voron/InitialFileSize";
            public const string TempPath = "Raven/Voron/TempPath";
            public const string MaxBufferPoolSize = "Raven/Voron/MaxBufferPoolSize";
            public const string InitialSize = "Raven/Voron/InitialSize";
            public const string MaxScratchBufferSize = "Raven/Voron/MaxScratchBufferSize";
        }

	    public class Versioning
	    {
			public const string RavenVersioningPrefix = "Raven/Versioning/";
			public const string RavenVersioningDefaultConfiguration = "Raven/Versioning/DefaultConfiguration";
	    }

	    public class SqlReplication
	    {
			public const string SqlReplicationConnectionsDocumentName = "Raven/SqlReplication/Connections";
	    }

        public class PeriodicExport
        {
            public const string AwsAccessKey = "Raven/AWSAccessKey";
            public const string AwsSecretKey = "Raven/AWSSecretKey";
            public const string AzureStorageAccount = "Raven/AzureStorageAccount";
            public const string AzureStorageKey = "Raven/AzureStorageKey";
        }

	    public class Global
	    {
		    public const string GlobalSettingsDocumentKey = "Raven/Global/Settings";

			public const string ReplicationConflictResolutionDocumentName = "Raven/Global/Replication/Config";
		    public const string ReplicationDestinationsDocumentName = "Raven/Global/Replication/Destinations";

		    public const string VersioningDocumentPrefix = "Raven/Global/Versioning/";
		    public const string VersioningDefaultConfigurationDocumentName = "Raven/Global/Versioning/DefaultConfiguration";

		    public const string PeriodicExportDocumentName = "Raven/Global/Backup/Periodic/Setup";

		    public const string SqlReplicationConnectionsDocumentName = "Raven/Global/SqlReplication/Connections";

		    public const string JavascriptFunctions = "Raven/Global/Javascript/Functions";
	    }

	    public static class Smuggler
	    {
		    public const string CallContext = "Raven/Smuggler/CallContext";
	    }
	}
}