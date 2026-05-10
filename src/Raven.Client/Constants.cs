using System;
using Raven.Client.Documents.Indexes.Vector;

namespace Raven.Client
{
    /// <summary>
    /// Contains constants used throughout the RavenDB client library.
    /// This static class provides categorized collections of constants for various RavenDB operations.
    /// </summary>
    public static class Constants
    {
        /// <summary>
        /// Contains constants related to JSON processing and serialization.
        /// </summary>
        public sealed class Json
        {
            private Json()
            {
            }

            /// <summary>
            /// Contains field names used in JSON serialization.
            /// </summary>
            public sealed class Fields
            {
                private Fields()
                {
                }

                /// <summary>
                /// The JSON field name used to specify the type information for polymorphic serialization.
                /// </summary>
                public const string Type = "$type";

                /// <summary>
                /// The JSON field name used to specify values in JSON collections.
                /// </summary>
                public const string Values = "$values";
            }
        }

        internal sealed class QueryString
        {
            private QueryString()
            {
            }

            public const string NodeTag = "nodeTag";

            public const string ShardNumber = "shardNumber";
        }

        /// <summary>
        /// Contains HTTP header names used in RavenDB client-server communication.
        /// </summary>
        public sealed class Headers
        {
            private Headers()
            {
            }

            /// <summary>
            /// HTTP header name for request timestamp.
            /// </summary>
            public const string RequestTime = "Request-Time";

            /// <summary>
            /// HTTP header name for server startup timestamp.
            /// </summary>
            public const string ServerStartupTime = "Server-Startup-Time";

            /// <summary>
            /// HTTP header name for triggering topology refresh.
            /// </summary>
            public const string RefreshTopology = "Refresh-Topology";

            /// <summary>
            /// HTTP header name for topology etag.
            /// </summary>
            public const string TopologyEtag = "Topology-Etag";

            /// <summary>
            /// HTTP header name for cluster topology etag.
            /// </summary>
            public const string ClusterTopologyEtag = "Cluster-Topology-Etag";

            /// <summary>
            /// HTTP header name for client configuration etag.
            /// </summary>
            public const string ClientConfigurationEtag = "Client-Configuration-Etag";

            /// <summary>
            /// HTTP header name for the last known cluster transaction index.
            /// </summary>
            public const string LastKnownClusterTransactionIndex = "Known-Raft-Index";

            /// <summary>
            /// HTTP header name for database cluster transaction identifier.
            /// </summary>
            public const string DatabaseClusterTransactionId = "Database-Cluster-Tx-Id";

            /// <summary>
            /// HTTP header name for triggering client configuration refresh.
            /// </summary>
            public const string RefreshClientConfiguration = "Refresh-Client-Configuration";

            /// <summary>
            /// HTTP header name for entity tag (ETag).
            /// </summary>
            public const string Etag = "ETag";

            /// <summary>
            /// HTTP header name for RavenDB client version.
            /// </summary>
            public const string ClientVersion = "Raven-Client-Version";

            /// <summary>
            /// HTTP header name for RavenDB server version.
            /// </summary>
            public const string ServerVersion = "Raven-Server-Version";

            /// <summary>
            /// HTTP header name for RavenDB Studio version.
            /// </summary>
            public const string StudioVersion = "Raven-Studio-Version";

            /// <summary>
            /// HTTP header name for conditional request matching.
            /// </summary>
            public const string IfMatch = "If-Match";

            /// <summary>
            /// HTTP header name for conditional request non-matching.
            /// </summary>
            public const string IfNoneMatch = "If-None-Match";

            /// <summary>
            /// HTTP header name for transfer encoding.
            /// </summary>
            public const string TransferEncoding = "Transfer-Encoding";

            /// <summary>
            /// HTTP header name for content encoding.
            /// </summary>
            public const string ContentEncoding = "Content-Encoding";

            /// <summary>
            /// HTTP header name for accept encoding.
            /// </summary>
            public const string AcceptEncoding = "Accept-Encoding";

            /// <summary>
            /// HTTP header name for content disposition.
            /// </summary>
            public const string ContentDisposition = "Content-Disposition";

            /// <summary>
            /// HTTP header name for content type.
            /// </summary>
            public const string ContentType = "Content-Type";

            /// <summary>
            /// HTTP header name for content length.
            /// </summary>
            public const string ContentLength = "Content-Length";

            /// <summary>
            /// HTTP header name for request origin.
            /// </summary>
            public const string Origin = "Origin";

            /// <summary>
            /// Prefix used for incremental time series headers.
            /// </summary>
            public const string IncrementalTimeSeriesPrefix = "INC:";

            internal const string Sharded = "Sharded";

            /// <summary>
            /// HTTP header name for attachment hash.
            /// </summary>
            public const string AttachmentHash = "Attachment-Hash";

            /// <summary>
            /// HTTP header name for attachment size.
            /// </summary>
            public const string AttachmentSize = "Attachment-Size";

            /// <summary>
            /// HTTP header name for the scheduled remote attachment upload time.
            /// </summary>
            /// <remarks>
            /// This header contains the timestamp (UTC) when an attachment should be uploaded to remote cloud storage.
            /// Used in conjunction with <see cref="AttachmentRemoteParametersFlags"/> and <see cref="AttachmentRemoteParametersIdentifier"/>
            /// to configure automatic offloading of attachments to cloud storage providers like Amazon S3 or Azure Blob Storage.
            /// </remarks>
            public const string AttachmentRemoteParametersAt = "Attachment-RemoteParameters-At";

            /// <summary>
            /// HTTP header name for remote attachment configuration flags.
            /// </summary>
            /// <remarks>
            /// This header contains flags that control remote attachment behavior, such as whether the attachment
            /// should be uploaded immediately or at a scheduled time. Used with <see cref="AttachmentRemoteParametersAt"/>
            /// and <see cref="AttachmentRemoteParametersIdentifier"/> to specify complete remote storage configuration.
            /// The flags correspond to the <see cref="Client.Documents.Attachments.RemoteAttachmentFlags"/> enum values.
            /// </remarks>
            public const string AttachmentRemoteParametersFlags = "Attachment-RemoteParameters-Flags";

            /// <summary>
            /// HTTP header name for remote attachment storage destination identifier.
            /// </summary>
            /// <remarks>
            /// This header contains the unique identifier for the remote storage destination configuration where the attachment
            /// should be uploaded. This identifier references a configured remote attachment destination (e.g., a specific S3 bucket
            /// or Azure container) in the database's remote attachments configuration. Used with <see cref="AttachmentRemoteParametersAt"/>
            /// and <see cref="AttachmentRemoteParametersFlags"/> to specify the complete remote storage parameters.
            /// </remarks>
            public const string AttachmentRemoteParametersIdentifier = "Attachment-RemoteParameters-Identifier";

            internal const string DatabaseMissing = "Database-Missing";

            internal const string CommandType = "Command-Type";

            internal const string AttachmentStream = "AttachmentStream";

            internal class Encodings
            {
                private Encodings()
                {
                }

                public const string Gzip = "gzip";

#if FEATURE_BROTLI_SUPPORT
                public const string Brotli = "br";
#endif

                public const string Deflate = "deflate";

#if FEATURE_ZSTD_SUPPORT
                public const string Zstd = "zstd";
#endif
            }
        }

        /// <summary>
        /// Contains platform-specific constants for different operating systems.
        /// </summary>
        public sealed class Platform
        {
            private Platform()
            {
            }

            /// <summary>
            /// Contains Windows-specific constants and limitations.
            /// </summary>
            public sealed class Windows
            {
                private Windows()
                {
                }

                /// <summary>
                /// Maximum path length supported on Windows.
                /// </summary>
                public static readonly int MaxPath = short.MaxValue;

                /// <summary>
                /// Reserved file names that cannot be used on Windows.
                /// </summary>
                internal static readonly string[] ReservedFileNames = {
                    "con",
                    "prn",
                    "aux",
                    "nul",
                    "com1",
                    "com2",
                    "com3",
                    "com4",
                    "com5",
                    "com6",
                    "com7",
                    "com8",
                    "com9",
                    "lpt1",
                    "lpt2",
                    "lpt3",
                    "lpt4",
                    "lpt5",
                    "lpt6",
                    "lpt7",
                    "lpt8",
                    "lpt9",
                    "clock$"
                };
            }

            /// <summary>
            /// Contains Linux-specific constants and limitations.
            /// </summary>
            public sealed class Linux
            {
                private Linux()
                {
                }

                /// <summary>
                /// Maximum path length supported on Linux.
                /// </summary>
                public const int MaxPath = 4096;

                /// <summary>
                /// Maximum file name length supported on Linux.
                /// </summary>
                public const int MaxFileNameLength = 230;
            }
        }

        /// <summary>
        /// Contains constants related to certificate management.
        /// </summary>
        public sealed class Certificates
        {
            private Certificates()
            {
            }

            /// <summary>
            /// The prefix used for certificate identifiers.
            /// </summary>
            public const string Prefix = "certificates/";
            
            /// <summary>
            /// Maximum number of certificates allowed with the same hash.
            /// </summary>
            public const int MaxNumberOfCertsWithSameHash = 5;
            internal const string ServerAuthenticationOid = "1.3.6.1.5.5.7.3.1";
            internal const string ClientAuthenticationOid = "1.3.6.1.5.5.7.3.2";
            internal const string ServerCertExtensionOid =  CompanyInformation.CompanyOid + ".2.1";
        }

        internal sealed class Network
        {
            public const string AnyIp = "0.0.0.0";
            public const int ZeroValue = 0;
            public const int DefaultSecuredRavenDbHttpPort = 443;
            public const int DefaultSecuredRavenDbTcpPort = 38888;
        }

        internal sealed class DatabaseSettings
        {
            private DatabaseSettings()
            {
            }

            public const string StudioId = "DatabaseSettings/Studio";
        }

        /// <summary>
        /// Contains constants related to RavenDB configuration.
        /// </summary>
        public sealed class Configuration
        {
            private Configuration()
            {
            }

            internal sealed class Indexes
            {
                internal const string IndexingStaticSearchEngineType = "Indexing.Static.SearchEngineType";
            }

            /// <summary>
            /// Configuration identifier for client settings.
            /// </summary>
            public const string ClientId = "Configuration/Client";

            /// <summary>
            /// Configuration identifier for studio settings.
            /// </summary>
            public const string StudioId = "Configuration/Studio";
        }

        /// <summary>
        /// Contains constants related to RavenDB counters.
        /// </summary>
        public static class Counters
        {
            /// <summary>
            /// Special identifier for all counters in a document.
            /// </summary>
            public const string All = "@all_counters";
        }

        /// <summary>
        /// Contains constants related to RavenDB time series.
        /// </summary>
        public static class TimeSeries
        {
            internal const string SelectFieldName = "timeseries";
            internal const string QueryFunction = "__timeSeriesQueryFunction";

            /// <summary>
            /// Special identifier for all time series in a document.
            /// </summary>
            public const string All = "@all_timeseries";
        }

        /// <summary>
        /// Contains constants related to RavenDB documents, collections, and metadata.
        /// </summary>
        public sealed class Documents
        {
            private Documents()
            {
            }

            /// <summary>
            /// The prefix used for database names in RavenDB.
            /// </summary>
            public const string Prefix = "db/";

            /// <summary>
            /// The maximum allowed length for a database name.
            /// </summary>
            public const int MaxDatabaseNameLength = 128;

            /// <summary>
            /// Defines special states for subscription change vectors.
            /// </summary>
            public enum SubscriptionChangeVectorSpecialStates
            {
                /// <summary>
                /// Indicates that the change vector should not be changed.
                /// </summary>
                DoNotChange,
                /// <summary>
                /// Indicates that the subscription should start from the last document.
                /// </summary>
                LastDocument,
                /// <summary>
                /// Indicates that the subscription should start from the beginning of time.
                /// </summary>
                BeginningOfTime
            }

            /// <summary>
            /// Contains constants for document metadata field names.
            /// </summary>
            public sealed class Metadata
            {
                private Metadata()
                {
                }

                /// <summary>
                /// Metadata field name for graph edges.
                /// </summary>
                public const string Edges = "@edges";

                /// <summary>
                /// Metadata field name for document collection.
                /// </summary>
                public const string Collection = "@collection";

                /// <summary>
                /// Metadata field name for query projections.
                /// </summary>
                public const string Projection = "@projection";

                /// <summary>
                /// Metadata field name for the metadata key.
                /// </summary>
                public const string Key = "@metadata";

                /// <summary>
                /// Metadata field name for document identifier.
                /// </summary>
                public const string Id = "@id";

                /// <summary>
                /// Metadata field name for conflict information.
                /// </summary>
                public const string Conflict = "@conflict";

                /// <summary>
                /// Property name for document identifier in objects.
                /// </summary>
                public const string IdProperty = "Id";

                /// <summary>
                /// Metadata field name for document flags.
                /// </summary>
                public const string Flags = "@flags";

                /// <summary>
                /// Metadata field name for document attachments.
                /// </summary>
                public const string Attachments = "@attachments";

                /// <summary>
                /// Metadata field name for document counters.
                /// </summary>
                public const string Counters = "@counters";

                /// <summary>
                /// Metadata field name for document time series.
                /// </summary>
                public const string TimeSeries = "@timeseries";

                /// <summary>
                /// Metadata field name for time series with named values.
                /// </summary>
                public const string TimeSeriesNamedValues = "@timeseries-named-values";

                /// <summary>
                /// Metadata field name for revision counters snapshot.
                /// </summary>
                public const string RevisionCounters = "@counters-snapshot";

                /// <summary>
                /// Metadata field name for revision time series snapshot.
                /// </summary>
                public const string RevisionTimeSeries = "@timeseries-snapshot";

                /// <summary>
                /// Metadata field name for legacy attachment metadata.
                /// </summary>
                public const string LegacyAttachmentsMetadata = "@legacy-attachment-metadata";

                /// <summary>
                /// Metadata field name for index score.
                /// </summary>
                public const string IndexScore = "@index-score";

                /// <summary>
                /// Metadata field name for spatial query results.
                /// </summary>
                public const string SpatialResult = "@spatial";

                /// <summary>
                /// Metadata field name for last modified timestamp.
                /// </summary>
                public const string LastModified = "@last-modified";

                /// <summary>
                /// Metadata field name for the .NET CLR type information.
                /// </summary>
                public const string RavenClrType = "Raven-Clr-Type";

                /// <summary>
                /// Metadata field name for document change vector.
                /// </summary>
                public const string ChangeVector = "@change-vector";

                public const string Expires = "@expires";

                public const string Refresh = "@refresh";

                public const string ArchiveAt = "@archive-at";

                public const string Archived = "@archived";

                public const string HasValue = "HasValue";

                public const string Etag = "@etag";
                
                public const string Quantization = "@quantization";

                internal const string GenAiHashes = "@gen-ai-hashes";

                internal sealed class Sharding
                {
                    internal const string ShardNumber = "@shard-number";

                    internal sealed class Querying
                    {
                        internal const string OrderByFields = "@order-by-fields";

                        internal const string SuggestionsPopularityFields = "@suggestions-popularity";

                        internal const string ResultDataHash = "@data-hash";
                    }

                    internal sealed class Subscription
                    {
                        internal const string NonPersistentFlags = "@non-persistent-flags";
                    }
                }
            }

            public sealed class Collections
            {
                public const string AllDocumentsCollection = "@all_docs";

                public const string EmptyCollection = "@empty";

                public const string EmbeddingsCacheCollection = "@embeddings-cache";

                public const string AiAgentConversationCollection = "@conversations";

                public const string AiAgentConversationHistoryCollection = "@conversations-history";

                public const string AiAgentConversationDebugCollection = "@conversations-debug";
            }

            internal sealed class Ai
            {
                public const string AiAgentIdPrefix = "Conversations";
            }

            public sealed class Indexing
            {
                private Indexing()
                {
                }

                public const string SideBySideIndexNamePrefix = "ReplacementOf/";

                public sealed class Fields
                {
                    private Fields()
                    {
                    }

                    public const string CountFieldName = "Count";

#if FEATURE_CUSTOM_SORTING
                    public const string CustomSortFieldName = "__customSort";
#endif

                    public const string DocumentIdFieldName = "id()";

                    public const string DocumentIdMethodName = "id";

                    public const string SourceDocumentIdFieldName = "sourceDocId()";

                    public const string ReduceKeyHashFieldName = "hash(key())";

                    public const string ReduceKeyValueFieldName = "key()";

                    public const string ValueFieldName = "value()";

                    public const string AllFields = "__all_fields";

                    public const string AllStoredFields = "__all_stored_fields";

                    public const string SpatialShapeFieldName = "spatial(shape)";

                    internal const string RangeFieldSuffix = "_Range";

                    public const string RangeFieldSuffixLong = "_L" + RangeFieldSuffix;

                    public const string RangeFieldSuffixDouble = "_D" + RangeFieldSuffix;

                    internal const string TimeFieldSuffix = "_Time";

                    public const string NullValue = "NULL_VALUE";

                    public const string EmptyString = "EMPTY_STRING";

                    public sealed class JavaScript
                    {
                        private JavaScript()
                        {
                        }

                        public const string ValuePropertyName = "$value";

                        public const string OptionsPropertyName = "$options";

                        public const string NamePropertyName = "$name";

                        public const string SpatialPropertyName = "$spatial";

                        public const string BoostPropertyName = "$boost";
                        
                        public const string VectorPropertyName = "$vector";
                        public const string LoadVectorPropertyName = "$loadvector";

                        internal const string LoadVectorEmbeddingSourceDocumentId = "$embeddingSourceDocumentId";
                        internal const string LoadVectorEmbeddingSourceDocumentCollectionName = "$embeddingSourceDocumentCollectionName";
                    }
                }

                public sealed class Spatial
                {
                    private Spatial()
                    {
                    }

                    public const double DefaultDistanceErrorPct = 0.025d;

                    /// <summary>
                    /// The International Union of Geodesy and Geophysics says the Earth's mean radius in KM is:
                    ///
                    /// [1] http://en.wikipedia.org/wiki/Earth_radius
                    /// </summary>
                    public const double EarthMeanRadiusKm = 6371.0087714;

                    public const double MilesToKm = 1.60934;
                }

                internal sealed class Analyzers
                {
                    private Analyzers()
                    {
                    }

                    public const string Default = "LowerCaseKeywordAnalyzer";

                    public const string DefaultExact = "KeywordAnalyzer";

                    public const string DefaultSearch = "RavenStandardAnalyzer";
                }
            }

            public sealed class Querying
            {
                private Querying()
                {
                }

                public sealed class Facet
                {
                    private Facet()
                    {
                    }

                    public const string AllResults = "@AllResults";
                }

                internal sealed class Fields
                {
                    internal const string PowerBIJsonFieldName = "json()";
                }

                public sealed class Sharding
                {
                    private Sharding()
                    {
                    }

                    internal const string ShardContextParameterName = "__shardContext";

                    internal const string ShardContextDocumentIds = "DocumentIds";

                    internal const string ShardContextPrefixes = "Prefixes";
                }
                
                public sealed class Terms
                {
                    internal const string LeftNullValueOfBetweenQuery = "*";
                    internal const string RightNullValueOfBetweenQuery = "NULL";
                }
            }

            public sealed class PeriodicBackup
            {
                private PeriodicBackup()
                {
                }

                public const string FullBackupExtension = ".ravendb-full-backup";

                public const string SnapshotExtension = ".ravendb-snapshot";

                public const string EncryptedFullBackupExtension = ".ravendb-encrypted-full-backup";

                public const string EncryptedSnapshotExtension = ".ravendb-encrypted-snapshot";

                public const string IncrementalBackupExtension = ".ravendb-incremental-backup";

                public const string EncryptedIncrementalBackupExtension = ".ravendb-encrypted-incremental-backup";

                public sealed class Folders
                {
                    private Folders()
                    {
                    }

                    public const string Indexes = "Indexes";

                    public const string Documents = "Documents";

                    public const string Configuration = "Configuration";
                }
            }

            internal sealed class Blob
            {
                public const string Document = "@raven-data";

                public const string Size = "@raven-blob-size";
            }
        }

        internal static class Identities
        {
            public const char DefaultSeparator = '/';
        }

        internal static class Smuggler
        {
            public const string ImportOptions = "importOptions";

            public const string CsvImportOptions = "csvImportOptions";
        }

        internal static class Operations
        {
            public const long InvalidOperationId = -1;
        }

        internal sealed class CompareExchange
        {
            private CompareExchange()
            {
            }

            public const string RvnAtomicPrefix = "rvn-atomic/";

            public const string ObjectFieldName = "Object";
        }

        internal sealed class Monitoring
        {
            private Monitoring()
            {
            }

            internal sealed class Snmp
            {
                private Snmp()
                {
                }

                public const string DatabasesMappingKey = "monitoring/snmp/databases/mapping";
                public const string SnmpRootOid = CompanyInformation.CompanyOid + ".1.1";
            }
        }

        internal sealed class Fields
        {
            private Fields()
            {
            }

            internal sealed class CommandData
            {
                private CommandData()
                {
                }

                public const string DocumentChangeVector = null;

                public const string DestinationDocumentChangeVector = null;
            }
        }

        internal sealed class Obsolete
        {
            private Obsolete()
            {
            }
        }

        internal class DatabaseRecord
        {
            private DatabaseRecord()
            {
            }

            internal class SupportedFeatures
            {
                private SupportedFeatures()
                {
                }

                public const string ThrowRevisionKeyTooBigFix = "ThrowRevisionKeyTooBigFix";
            }
        }

        internal static class VectorSearch
        {
            internal const string AiTaskMethodName = "ai.task";
            private const string EmbeddingPrefix = "embedding.";
            
            internal const string EmbeddingForDocument = EmbeddingPrefix + "forDoc";
            internal const string EmbeddingForRaw = EmbeddingPrefix + "Raw";
            internal const string EmbeddingText = EmbeddingPrefix + "text";
            internal const string EmbeddingTextInt8 = EmbeddingPrefix + "text_i8";
            internal const string EmbeddingTextInt1 = EmbeddingPrefix + "text_i1";
            internal const string EmbeddingSingle = EmbeddingPrefix + "f32";
            internal const string EmbeddingSingleInt8 = EmbeddingPrefix + "f32_i8";
            internal const string EmbeddingSingleInt1 = EmbeddingPrefix + "f32_i1";
            internal const string EmbeddingInt8 = EmbeddingPrefix + "i8";
            internal const string EmbeddingInt1 = EmbeddingPrefix + "i1";

            internal static string ConfigurationToMethodName(VectorEmbeddingType source, VectorEmbeddingType dest) => (source, dest) switch
            {
                (VectorEmbeddingType.Single, VectorEmbeddingType.Single) => string.Empty,
                (VectorEmbeddingType.Single, VectorEmbeddingType.Int8) => EmbeddingSingleInt8,
                (VectorEmbeddingType.Single, VectorEmbeddingType.Binary) => EmbeddingSingleInt1,
                (VectorEmbeddingType.Text, VectorEmbeddingType.Single) => EmbeddingText,
                (VectorEmbeddingType.Text, VectorEmbeddingType.Int8) => EmbeddingTextInt8,
                (VectorEmbeddingType.Text, VectorEmbeddingType.Binary) => EmbeddingTextInt1,
                (VectorEmbeddingType.Int8, VectorEmbeddingType.Int8) => EmbeddingInt8,
                (VectorEmbeddingType.Binary, VectorEmbeddingType.Binary) => EmbeddingInt1,
                _ => throw new InvalidOperationException($"Invalid embedding configuration. SourceEmbedding: {source}, DestinationEmbedding: {dest}")
            };
            

            public const VectorEmbeddingType DefaultEmbeddingType = VectorEmbeddingType.Single;
            public const bool DefaultIsExact = false;
        }

        internal class CompanyInformation
        {
            private CompanyInformation()
            {
            }

            public const string CompanyOid = "1.3.6.1.4.1.45751";
        }
    }
}
