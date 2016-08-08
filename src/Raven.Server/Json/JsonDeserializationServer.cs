using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Replication;
using Raven.Client.Indexing;
using Raven.Client.Json;
using Raven.Client.Replication;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.PeriodicExport;
using Raven.Server.Documents.SqlReplication;
using Raven.Server.Documents.Versioning;
using Sparrow.Json;

namespace Raven.Server.Json
{
    // TODO: Use JsonDeserialization for the helper methods instead of duplicating
    public class JsonDeserializationServer : JsonDeserialization
    {
        private static readonly Type[] EmptyTypes = new Type[0];

        public static readonly Func<BlittableJsonReaderObject, ReplicationBatchReply> ReplicationBatchReply = GenerateJsonDeserializationRoutine<ReplicationBatchReply>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationLatestEtagRequest> ReplicationLatestEtagRequest = GenerateJsonDeserializationRoutine<ReplicationLatestEtagRequest>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationLatestEtagReply> ReplicationEtagReply = GenerateJsonDeserializationRoutine<ReplicationLatestEtagReply>();


        public static readonly Func<BlittableJsonReaderObject, SubscriptionConnectionClientMessage> SubscriptionConnectionClientMessage = GenerateJsonDeserializationRoutine<SubscriptionConnectionClientMessage>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionConnectionOptions> SubscriptionConnectionOptions = GenerateJsonDeserializationRoutine<SubscriptionConnectionOptions>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationClientConfiguration> ReplicationClientConfiguration = GenerateJsonDeserializationRoutine<ReplicationClientConfiguration>();
        public static readonly Func<BlittableJsonReaderObject, ReplicationDocument> ReplicationDocument = GenerateJsonDeserializationRoutine<ReplicationDocument>();
        public static readonly Func<BlittableJsonReaderObject, ReplicationDestination> ReplicationDestination = GenerateJsonDeserializationRoutine<ReplicationDestination>();


        public static readonly Func<BlittableJsonReaderObject, SqlReplicationConfiguration> SqlReplicationConfiguration = GenerateJsonDeserializationRoutine<SqlReplicationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, SimulateSqlReplication> SimulateSqlReplication = GenerateJsonDeserializationRoutine<SimulateSqlReplication>();

        public static readonly Func<BlittableJsonReaderObject, PredefinedSqlConnection> PredefinedSqlConnection = GenerateJsonDeserializationRoutine<PredefinedSqlConnection>();

        public static readonly Func<BlittableJsonReaderObject, SqlReplicationTable> SqlReplicationTable = GenerateJsonDeserializationRoutine<SqlReplicationTable>();

        public static readonly Func<BlittableJsonReaderObject, SqlReplicationStatus> SqlReplicationStatus = GenerateJsonDeserializationRoutine<SqlReplicationStatus>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionCriteria> SubscriptionCriteria = GenerateJsonDeserializationRoutine<SubscriptionCriteria>();
        public static readonly Func<BlittableJsonReaderObject, VersioningConfigurationCollection> VersioningConfigurationCollection = GenerateJsonDeserializationRoutine<VersioningConfigurationCollection>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionConnectionOptions> SubscriptionCriteriaOptions = GenerateJsonDeserializationRoutine<SubscriptionConnectionOptions>();
        public static readonly Func<BlittableJsonReaderObject, VersioningConfiguration> VersioningConfiguration = GenerateJsonDeserializationRoutine<VersioningConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, ExpirationConfiguration> ExpirationConfiguration = GenerateJsonDeserializationRoutine<ExpirationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, PeriodicExportConfiguration> PeriodicExportConfiguration = GenerateJsonDeserializationRoutine<PeriodicExportConfiguration>();
        public static readonly Func<BlittableJsonReaderObject, PeriodicExportStatus> PeriodicExportStatus = GenerateJsonDeserializationRoutine<PeriodicExportStatus>();

        public static readonly Func<BlittableJsonReaderObject, SpatialOptions> SpatialOptions = GenerateJsonDeserializationRoutine<SpatialOptions>();

        public static readonly Func<BlittableJsonReaderObject, IndexDefinition> IndexDefinition = GenerateJsonDeserializationRoutine<IndexDefinition>();

        public static readonly Func<BlittableJsonReaderObject, TransformerDefinition> TransformerDefinition = GenerateJsonDeserializationRoutine<TransformerDefinition>();

        private new static Func<BlittableJsonReaderObject, T> GenerateJsonDeserializationRoutine<T>(Type deserializorType = null)
        {
            return JsonDeserialization.GenerateJsonDeserializationRoutine<T>(typeof(JsonDeserializationServer));
        }
    }
}