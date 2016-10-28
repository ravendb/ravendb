using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Replication;
using Raven.Client.Data;
using Raven.Client.Indexing;
using Raven.Client.Replication.Messages;
using Raven.Client.Smuggler;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.PeriodicExport;
using Raven.Server.Documents.SqlReplication;
using Raven.Server.Documents.Versioning;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Json
{
    public class JsonDeserializationServer : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, DatabaseExportOptions> DatabaseExportOptions = GenerateJsonDeserializationRoutine<DatabaseExportOptions>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationMessageReply> ReplicationMessageReply = GenerateJsonDeserializationRoutine<ReplicationMessageReply>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationLatestEtagRequest> ReplicationLatestEtagRequest = GenerateJsonDeserializationRoutine<ReplicationLatestEtagRequest>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionConnectionClientMessage> SubscriptionConnectionClientMessage = GenerateJsonDeserializationRoutine<SubscriptionConnectionClientMessage>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionConnectionOptions> SubscriptionConnectionOptions = GenerateJsonDeserializationRoutine<SubscriptionConnectionOptions>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationDocument> ReplicationDocument = GenerateJsonDeserializationRoutine<ReplicationDocument>();


        public static readonly Func<BlittableJsonReaderObject, SqlReplicationConfiguration> SqlReplicationConfiguration = GenerateJsonDeserializationRoutine<SqlReplicationConfiguration>();
        public static readonly Func<BlittableJsonReaderObject, SqlReplicationStatus> SqlReplicationStatus = GenerateJsonDeserializationRoutine<SqlReplicationStatus>();
        public static readonly Func<BlittableJsonReaderObject, SimulateSqlReplication> SimulateSqlReplication = GenerateJsonDeserializationRoutine<SimulateSqlReplication>();
        public static readonly Func<BlittableJsonReaderObject, PredefinedSqlConnection> PredefinedSqlConnection = GenerateJsonDeserializationRoutine<PredefinedSqlConnection>();


        public static readonly Func<BlittableJsonReaderObject, SubscriptionCriteria> SubscriptionCriteria = GenerateJsonDeserializationRoutine<SubscriptionCriteria>();

        public static readonly Func<BlittableJsonReaderObject, VersioningConfiguration> VersioningConfiguration = GenerateJsonDeserializationRoutine<VersioningConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, ExpirationConfiguration> ExpirationConfiguration = GenerateJsonDeserializationRoutine<ExpirationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, PeriodicExportConfiguration> PeriodicExportConfiguration = GenerateJsonDeserializationRoutine<PeriodicExportConfiguration>();
        public static readonly Func<BlittableJsonReaderObject, PeriodicExportStatus> PeriodicExportStatus = GenerateJsonDeserializationRoutine<PeriodicExportStatus>();

        public static readonly Func<BlittableJsonReaderObject, IndexDefinition> IndexDefinition = GenerateJsonDeserializationRoutine<IndexDefinition>();
        internal static readonly Func<BlittableJsonReaderObject, LegacyIndexDefinition> LegacyIndexDefinition = GenerateJsonDeserializationRoutine<LegacyIndexDefinition>();

        public static readonly Func<BlittableJsonReaderObject, TransformerDefinition> TransformerDefinition = GenerateJsonDeserializationRoutine<TransformerDefinition>();

        public static readonly Func<BlittableJsonReaderObject, FacetSetup> FacetSetup = GenerateJsonDeserializationRoutine<FacetSetup>();
        public static readonly Func<BlittableJsonReaderObject, Facet> Facet = GenerateJsonDeserializationRoutine<Facet>();
    }
}