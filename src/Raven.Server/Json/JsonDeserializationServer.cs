using System;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Documents.Transformers;
using Raven.Client.Server;
using Raven.Client.Server.Tcp;
using Raven.Server.Commercial;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.PeriodicExport;
using Raven.Server.Documents.Studio;
using Raven.Server.Documents.Versioning;
using Raven.Server.ServerWide.BackgroundTasks;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Facet = Raven.Client.Documents.Queries.Facets.Facet;
using FacetSetup = Raven.Client.Documents.Queries.Facets.FacetSetup;

namespace Raven.Server.Json
{
    internal class JsonDeserializationServer : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, TopologyDiscoveryRequest> TopologyDiscoveryRequest =
            GenerateJsonDeserializationRoutine<TopologyDiscoveryRequest>();

        public static readonly Func<BlittableJsonReaderObject, TopologyDiscoveryResponseHeader> TopologyDiscoveryResponse =
            GenerateJsonDeserializationRoutine<TopologyDiscoveryResponseHeader>();

        public static readonly Func<BlittableJsonReaderObject, NodeTopologyInfo> NodeTopologyInfo =
            GenerateJsonDeserializationRoutine<NodeTopologyInfo>();

        public static readonly Func<BlittableJsonReaderObject, FullTopologyInfo> FullTopologyInfo =
            GenerateJsonDeserializationRoutine<FullTopologyInfo>();

        public static readonly Func<BlittableJsonReaderObject, ActiveNodeStatus> ActiveNodeStatus =
            GenerateJsonDeserializationRoutine<ActiveNodeStatus>();

        public static readonly Func<BlittableJsonReaderObject, InactiveNodeStatus> InactiveNodeStatus =
            GenerateJsonDeserializationRoutine<InactiveNodeStatus>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseSmugglerOptions> DatabaseSmugglerOptions = GenerateJsonDeserializationRoutine<DatabaseSmugglerOptions>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationMessageReply> ReplicationMessageReply = GenerateJsonDeserializationRoutine<ReplicationMessageReply>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionHeaderResponse> TcpConnectionHeaderResponse = GenerateJsonDeserializationRoutine<TcpConnectionHeaderResponse>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationLatestEtagRequest> ReplicationLatestEtagRequest = GenerateJsonDeserializationRoutine<ReplicationLatestEtagRequest>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionConnectionClientMessage> SubscriptionConnectionClientMessage = GenerateJsonDeserializationRoutine<SubscriptionConnectionClientMessage>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionConnectionOptions> SubscriptionConnectionOptions = GenerateJsonDeserializationRoutine<SubscriptionConnectionOptions>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationDocument> ReplicationDocument = GenerateJsonDeserializationRoutine<ReplicationDocument>();

        public static readonly Func<BlittableJsonReaderObject, EtlDestinationsConfig> EtlConfiguration = GenerateJsonDeserializationRoutine<EtlDestinationsConfig>();
        public static readonly Func<BlittableJsonReaderObject, EtlProcessStatus> EtlProcessStatus = GenerateJsonDeserializationRoutine<EtlProcessStatus>();

        public static readonly Func<BlittableJsonReaderObject, SqlDestination> SqlReplicationConfiguration = GenerateJsonDeserializationRoutine<SqlDestination>();
        public static readonly Func<BlittableJsonReaderObject, SimulateSqlEtl> SimulateSqlReplication = GenerateJsonDeserializationRoutine<SimulateSqlEtl>();

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

        public static readonly Func<BlittableJsonReaderObject, LatestVersionCheck.VersionInfo> LatestVersionCheckVersionInfo = GenerateJsonDeserializationRoutine<LatestVersionCheck.VersionInfo>();

        public static readonly Func<BlittableJsonReaderObject, License> License = GenerateJsonDeserializationRoutine<License>();

        public static readonly Func<BlittableJsonReaderObject, UserRegistrationInfo> UserRegistrationInfo = GenerateJsonDeserializationRoutine<UserRegistrationInfo>();

        public static readonly Func<BlittableJsonReaderObject, FeedbackForm> FeedbackForm = GenerateJsonDeserializationRoutine<FeedbackForm>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseDocument> DatabaseDocument = GenerateJsonDeserializationRoutine<DatabaseDocument>();
    }
}