using System;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Documents.Transformers;
using Raven.Client.Server;
using Raven.Client.Server.Commands;
using Raven.Client.Server.Expiration;
using Raven.Client.Server.PeriodicBackup;
using Raven.Client.Server.Tcp;
using Raven.Client.Server.Versioning;
using Raven.Server.Commercial;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Studio;
using Raven.Server.ServerWide.BackgroundTasks;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Facet = Raven.Client.Documents.Queries.Facets.Facet;
using FacetSetup = Raven.Client.Documents.Queries.Facets.FacetSetup;

namespace Raven.Server.Json
{
    internal class JsonDeserializationServer : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, DatabaseStatusReport> DatabaseStatusReport =
            GenerateJsonDeserializationRoutine<DatabaseStatusReport>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionInfo> TcpConnectionInfo =
            GenerateJsonDeserializationRoutine<TcpConnectionInfo>();

        public static readonly Func<BlittableJsonReaderObject, TopologyDiscoveryRequest> TopologyDiscoveryRequest =
            GenerateJsonDeserializationRoutine<TopologyDiscoveryRequest>();

        public static readonly Func<BlittableJsonReaderObject, TopologyDiscoveryResponseHeader> TopologyDiscoveryResponse =
            GenerateJsonDeserializationRoutine<TopologyDiscoveryResponseHeader>();

        public static readonly Func<BlittableJsonReaderObject, NodeTopologyInfo> NodeTopologyInfo =
            GenerateJsonDeserializationRoutine<NodeTopologyInfo>();

        public static readonly Func<BlittableJsonReaderObject, LiveTopologyInfo> LiveTopologyInfo =
            GenerateJsonDeserializationRoutine<LiveTopologyInfo>();

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

        public static readonly Func<BlittableJsonReaderObject, ConflictSolver> ConflictSolver = GenerateJsonDeserializationRoutine<ConflictSolver>();
        public static readonly Func<BlittableJsonReaderObject, ScriptResolver> ScriptResolver = GenerateJsonDeserializationRoutine<ScriptResolver>();

        public static readonly Func<BlittableJsonReaderObject, EtlDestinationsConfig> EtlConfiguration = GenerateJsonDeserializationRoutine<EtlDestinationsConfig>();
        public static readonly Func<BlittableJsonReaderObject, EtlProcessStatus> EtlProcessStatus = GenerateJsonDeserializationRoutine<EtlProcessStatus>();

        public static readonly Func<BlittableJsonReaderObject, SqlDestination> SqlReplicationConfiguration = GenerateJsonDeserializationRoutine<SqlDestination>();
        public static readonly Func<BlittableJsonReaderObject, SimulateSqlEtl> SimulateSqlReplication = GenerateJsonDeserializationRoutine<SimulateSqlEtl>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionCreationOptions> SubscriptionCreationParams = GenerateJsonDeserializationRoutine<SubscriptionCreationOptions>();

        public static readonly Func<BlittableJsonReaderObject, VersioningConfiguration> VersioningConfiguration = GenerateJsonDeserializationRoutine<VersioningConfiguration>();
    
        public static readonly Func<BlittableJsonReaderObject, ExpirationConfiguration> ExpirationConfiguration = GenerateJsonDeserializationRoutine<ExpirationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, PeriodicBackupConfiguration> PeriodicBackupConfiguration = GenerateJsonDeserializationRoutine<PeriodicBackupConfiguration>();
        public static readonly Func<BlittableJsonReaderObject, PeriodicBackupStatus> PeriodicBackupStatus = GenerateJsonDeserializationRoutine<PeriodicBackupStatus>();

        public static readonly Func<BlittableJsonReaderObject, IndexDefinition> IndexDefinition = GenerateJsonDeserializationRoutine<IndexDefinition>();
        internal static readonly Func<BlittableJsonReaderObject, LegacyIndexDefinition> LegacyIndexDefinition = GenerateJsonDeserializationRoutine<LegacyIndexDefinition>();

        public static readonly Func<BlittableJsonReaderObject, TransformerDefinition> TransformerDefinition = GenerateJsonDeserializationRoutine<TransformerDefinition>();

        public static readonly Func<BlittableJsonReaderObject, FacetSetup> FacetSetup = GenerateJsonDeserializationRoutine<FacetSetup>();
        public static readonly Func<BlittableJsonReaderObject, Facet> Facet = GenerateJsonDeserializationRoutine<Facet>();

        public static readonly Func<BlittableJsonReaderObject, LatestVersionCheck.VersionInfo> LatestVersionCheckVersionInfo = GenerateJsonDeserializationRoutine<LatestVersionCheck.VersionInfo>();

        public static readonly Func<BlittableJsonReaderObject, License> License = GenerateJsonDeserializationRoutine<License>();

        public static readonly Func<BlittableJsonReaderObject, UserRegistrationInfo> UserRegistrationInfo = GenerateJsonDeserializationRoutine<UserRegistrationInfo>();

        public static readonly Func<BlittableJsonReaderObject, FeedbackForm> FeedbackForm = GenerateJsonDeserializationRoutine<FeedbackForm>();
        public static readonly Func<BlittableJsonReaderObject, CustomIndexPaths> CustomIndexPaths = GenerateJsonDeserializationRoutine<CustomIndexPaths>();
    }
}