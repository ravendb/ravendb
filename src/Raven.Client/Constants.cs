using System.IO;
using System.Reflection;

namespace Raven.Client
{
    public static class Constants
    {
        public class Json
        {
            public const string CustomFunctionsId = "Raven/Javascript/Functions";

            private Json()
            {
            }

            public class Fields
            {
                private Fields()
                {
                }

                public const string Type = "$type";

                public const string Values = "$values";
            }
        }

        public class Headers
        {
            private Headers()
            {
            }

            public const string RequestTime = "Raven-Request-Time";

            public const string Etag = "ETag";
        }

        public class Platform
        {
            private Platform()
            {
            }

            public class Windows
            {
                private Windows()
                {
                }

                public static readonly int MaxPath = (int)typeof(Path).GetField("MaxLongPath", BindingFlags.Static | BindingFlags.NonPublic).GetValue(null);

                public static readonly string[] ReservedFileNames = { "con", "prn", "aux", "nul", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9", "clock$" };
            }

            public class Linux
            {
                private Linux()
                {
                }

                public const int MaxPath = 4096;

                public const int MaxFileNameLength = 230;
            }
        }

        public class ApiKeys
        {
            private ApiKeys()
            {
            }

            public const string Prefix = "apikeys/";
        }

        public static class Documents
        {
            public const string Prefix = "db/";

            public const string UrlPrefix = "databases";

            public const int MaxDatabaseNameLength = 128;

            public class Metadata
            {
                private Metadata()
                {
                }

                public const string Collection = "@collection";

                public const string Key = "@metadata";

                public const string Id = "@id";

                public const string IdProperty = "Id";

                public const string Etag = "@etag";

                public const string Flags = "@flags";

                public const string Attachments = "@attachments";

                public const string IndexScore = "@index-score";

                public const string LastModified = "@last-modified";

                public const string RavenClrType = "Raven-Clr-Type";

                public const string ChangeVector = "@change-vector";

                public const string HasValue = "HasValue";
            }

            public class BulkInsert
            {
                private BulkInsert()
                {
                }

                public const string Content = "Content";
            }

            public class Collections
            {
                public const string AllDocumentsCollection = "@all_docs";
            }

            public class Indexing
            {
                private Indexing()
                {
                }
                
                public const string SideBySideIndexNamePrefix = "ReplacementOf/";

                public class Fields
                {
                    private Fields()
                    {
                    }

                    public const string AlphaNumericFieldName = "__alphaNumeric";

                    public const string RandomFieldName = "__random";

                    public const string CustomSortFieldName = "__customSort";

                    public const string DocumentIdFieldName = "__document_id";

                    public const string ReduceKeyFieldName = "__reduce_key";

                    public const string ReduceValueFieldName = "__reduced_val";

                    public const string AllFields = "__all_fields";

                    public const string AllStoredFields = "__all_stored_fields";

                    public const string DefaultSpatialFieldName = "__spatial";

                    public const string SpatialShapeFieldName = "__spatial_shape";

                    public const string DistanceFieldName = "__distance";

                    public const string IndexFieldScoreName = "__field_score";

                    internal const string RangeFieldSuffix = "_Range";

                    public const string RangeFieldSuffixLong = "_L" + RangeFieldSuffix;

                    public const string RangeFieldSuffixDouble = "_D" + RangeFieldSuffix;

                    public const string IgnoredDynamicField = "__ignored";

                    public const string NullValueNotAnalyzed = "[[NULL_VALUE]]";

                    public const string EmptyStringNotAnalyzed = "[[EMPTY_STRING]]";

                    public const string NullValue = "NULL_VALUE";

                    public const string EmptyString = "EMPTY_STRING";
                }

                public class Spatial
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
            }
            
            public class Querying
            {
                private Querying()
                {
                }

                public const string IntersectSeparator = " INTERSECT ";
            }

            public class Encryption
            {
                private Encryption()
                {
                }

                public const int DefaultGeneratedEncryptionKeyLength = 256 / 8;

                public const string DataCouldNotBeDecrypted = "<data could not be decrypted>";
            }

            public class Replication
            {
                public const string ReplicationDestinationsId = "Raven/Replication/Destinations";

                public const string ReplicationConfigurationDocument = "Raven/Replication/Documents/Configuration";
            }

            public class Versioning
            {
                public const string ConfigurationId = "Raven/Versioning/Configuration";
            }

            public class ETL
            {
                public const string RavenEtlProcessStatusPrefix = "Raven/Etl/Status/";
                public const string RavenEtlDocument = "Raven/ETL";
            }

            public class PeriodicExport
            {

                public const string IncrementalExportExtension = ".ravendb-incremental-export";

                public const string FullExportExtension = ".ravendb-full-export";
            }

            public class Expiration
            {
                public const string ExpirationDate = "Raven-Expiration-Date";
            }
        }
    }
}
