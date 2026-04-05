using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public abstract class FilteredPullReplicationClusterConvergenceTestBase : ReplicationTestBase
{
    protected FilteredPullReplicationClusterConvergenceTestBase(ITestOutputHelper output) : base(output)
    {
    }

    private protected async Task ExecuteScenarioAsync(Options options, ScenarioId scenarioId, BridgeTicketMutationMode mutationMode = BridgeTicketMutationMode.None)
    {
        await using var lab = await ReplicationLab.CreateAsync(owner: this, options, ScenarioCatalog.Definitions.Value[scenarioId], mutationMode);

        ScenarioExecutionReport report = null;
        try
        {
            report = await lab.RunUntilStateVerifiedAsync();
            lab.AssertVerifiedState(report);
            await lab.VerifyReplicationAliveAfterVerificationAsync();
        }
        catch (Exception e)
        {
            report ??= await lab.CaptureReportAsync();
            throw new Xunit.Sdk.XunitException(ScenarioFailureReportBuilder.Build(report, e));
        }
    }

    private sealed class ReplicationLab : IAsyncDisposable
    {
        private const string InternalPrefix = "internal//";
        private const string TicketPrefix = "tickets//";
        private const int WarmupDocumentCount = 8;
        private readonly FilteredPullReplicationClusterConvergenceTestBase _owner;
        private readonly ScenarioDefinition _definition;
        private readonly List<ScenarioCheckpoint> _timeline = [];
        private readonly List<IDocumentStore> _storesToDispose = [];
        private readonly List<X509Certificate2> _certificatesToDispose = [];
        private readonly Dictionary<ClusterNode, RavenServer> _servers;
        private readonly Dictionary<ClusterNode, DocumentDatabase> _databases;
        private readonly Dictionary<ClusterNode, IDocumentStore> _stores;
        private readonly Dictionary<(ClusterNode Source, ClusterNode Target), InternalLinkBlocker> _retainedBlockers = new();
        private readonly string _databaseName;
        private readonly string _documentIdSuffix;
        private readonly string _ticketWildcard;
        private readonly BridgeTicketMutationMode _mutationMode;
        private readonly TestCertificatesHolder _certificates;
        private readonly string _pullCertificateBase64;
        private readonly string _pullCertificatePfxBase64;
        private readonly IDocumentStore _hubStore;

        private static string GetHubDefinitionName(ClusterNode node) => $"-hub-{node.ToString().ToLowerInvariant()}";
        private static string GetHubAccessName(ClusterNode node) => $"Access-{node}";
        private static string GetBridgeConnectionName(int stepIndex, ClusterNode source, ClusterNode target, string role) => $"-{role}-{source}-{target}-{stepIndex + 1}";
        private string GetBootstrapDocumentId(ClusterNode node) => AppendDocumentIdSuffix($"{InternalPrefix}bootstrap/{node}", _documentIdSuffix);
        private string GetScenarioDocumentId() => AppendDocumentIdSuffix($"{InternalPrefix}{_definition.Id}/document", _documentIdSuffix);
        private string GetTicketDocumentId(int stepIndex) => AppendDocumentIdSuffix($"{TicketPrefix}{_definition.Id}/step-{stepIndex + 1}", _documentIdSuffix);
        private string GetAliveMarkerDocumentId() => AppendDocumentIdSuffix($"{InternalPrefix}{_definition.Id}/alive", _documentIdSuffix);
        private string GetWarmupDocumentId(int stepIndex, ClusterNode writer, int ordinal) => AppendDocumentIdSuffix($"{InternalPrefix}{_definition.Id}/warmup/{stepIndex + 1}/{writer}/{ordinal:D2}", _documentIdSuffix);
        private string GetBridgeDatabaseName(int stepIndex, ClusterNode source, ClusterNode target) => $"{_databaseName}-bridge-{source}-{target}-{stepIndex + 1}-{Guid.NewGuid():N}";

        private ReplicationLab(
            FilteredPullReplicationClusterConvergenceTestBase owner,
            ScenarioDefinition definition,
            string databaseName,
            string documentIdSuffix,
            BridgeTicketMutationMode mutationMode,
            TestCertificatesHolder certificates,
            string pullCertificateBase64,
            string pullCertificatePfxBase64,
            IDocumentStore hubStore,
            Dictionary<ClusterNode, RavenServer> servers,
            Dictionary<ClusterNode, DocumentDatabase> databases,
            Dictionary<ClusterNode, IDocumentStore> stores)
        {
            _owner = owner;
            _definition = definition;
            _databaseName = databaseName;
            _documentIdSuffix = documentIdSuffix;
            _mutationMode = mutationMode;
            _certificates = certificates;
            _pullCertificateBase64 = pullCertificateBase64;
            _pullCertificatePfxBase64 = pullCertificatePfxBase64;
            _hubStore = hubStore;
            _servers = servers;
            _databases = databases;
            _stores = stores;
            _ticketWildcard = TicketPrefix + "*";
        }

        public static async Task<ReplicationLab> CreateAsync(FilteredPullReplicationClusterConvergenceTestBase owner, Options options, ScenarioDefinition definition, BridgeTicketMutationMode mutationMode)
        {
            (List<RavenServer> hubNodes, RavenServer hubLeader, TestCertificatesHolder certs) = await owner.CreateRaftClusterWithSsl(numberOfNodes: 3, watcherCluster: true);
            var databaseName = owner.GetDatabaseName();
            var adjustedOptions = owner.Replication.AdjustOptionsToClusterSize(new Options(options), hubLeader, clusterSize: 3);

            adjustedOptions.AdminCertificate = certs.ServerCertificateForCommunication.Value;
            adjustedOptions.ClientCertificate = certs.ServerCertificateForCommunication.Value;
            adjustedOptions.ModifyDatabaseName = _ => databaseName;
            adjustedOptions.CreateDatabase = true;

            var hubStore = owner.GetDocumentStore(adjustedOptions);
            var documentIdSuffix = await GetDocumentIdSuffixAsync(owner, hubStore, adjustedOptions.DatabaseMode);
            var scenarioDocumentId = AppendDocumentIdSuffix($"{InternalPrefix}{definition.Id}/document", documentIdSuffix);

            var nodeStores = owner.Cluster.GetDocumentStores(
                nodes: [hubNodes[0], hubNodes[1], hubNodes[2]],
                databaseName,
                disableTopologyUpdates: true,
                certificate: certs.ServerCertificateForCommunication.Value);
            var dbA = await owner.GetDocumentDatabaseInstanceForAsync(nodeStores[0], adjustedOptions.DatabaseMode, scenarioDocumentId, hubNodes[0]);
            var dbB = await owner.GetDocumentDatabaseInstanceForAsync(nodeStores[1], adjustedOptions.DatabaseMode, scenarioDocumentId, hubNodes[1]);
            var dbC = await owner.GetDocumentDatabaseInstanceForAsync(nodeStores[2], adjustedOptions.DatabaseMode, scenarioDocumentId, hubNodes[2]);
            var pullCertificate = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path),
                password: (string)null,
                X509KeyStorageFlags.Exportable);

            var lab = new ReplicationLab(
                owner,
                definition,
                databaseName,
                documentIdSuffix,
                mutationMode,
                certs,
                Convert.ToBase64String(pullCertificate.Export(X509ContentType.Cert)),
                Convert.ToBase64String(pullCertificate.Export(X509ContentType.Pfx)),
                hubStore,
                servers: new Dictionary<ClusterNode, RavenServer>
                {
                    [ClusterNode.A] = hubNodes[0],
                    [ClusterNode.B] = hubNodes[1],
                    [ClusterNode.C] = hubNodes[2]
                },
                databases: new Dictionary<ClusterNode, DocumentDatabase>
                {
                    [ClusterNode.A] = dbA,
                    [ClusterNode.B] = dbB,
                    [ClusterNode.C] = dbC
                },
                stores: new Dictionary<ClusterNode, IDocumentStore>
                {
                    [ClusterNode.A] = nodeStores[0],
                    [ClusterNode.B] = nodeStores[1],
                    [ClusterNode.C] = nodeStores[2]
                });

            lab._storesToDispose.Add(hubStore);
            lab._storesToDispose.AddRange(nodeStores);
            lab._certificatesToDispose.Add(pullCertificate);

            await lab.ConfigureHubDefinitionsAsync();
            await lab.PrimeClusterAsync();
            lab.EnsureInternalHandlersReady();
            lab.Record(CheckpointKind.ClusterReady, stepNumber: 0, detail: "cluster and pull-replication hub definitions are ready");
            return lab;
        }

        private static string AppendDocumentIdSuffix(string documentId, string documentIdSuffix)
        {
            if (string.IsNullOrEmpty(documentIdSuffix))
                return documentId;

            return documentId + documentIdSuffix;
        }

        private static async Task<string> GetDocumentIdSuffixAsync(
            FilteredPullReplicationClusterConvergenceTestBase owner,
            IDocumentStore store,
            RavenDatabaseMode databaseMode)
        {
            if (databaseMode != RavenDatabaseMode.Sharded)
                return null;

            var sharding = await owner.Sharding.GetShardingConfigurationAsync(store);
            var shardNumber = sharding.Shards.Keys.Min();
            return "$" + owner.Sharding.GetRandomIdForShard(sharding, shardNumber);
        }

        public async Task<ScenarioExecutionReport> RunUntilStateVerifiedAsync()
        {
            for (var stepIndex = 0; stepIndex < _definition.Steps.Count; stepIndex++)
                await ExecuteStepAsync(stepIndex, _definition.Steps[stepIndex]);

            Record(CheckpointKind.FinalStateVerified, _definition.Steps.Count, detail: "scenario run completed");
            return await CaptureReportAsync();
        }

        public async Task VerifyReplicationAliveAfterVerificationAsync()
        {
            ReleaseRetainedBlockers();
            Record(CheckpointKind.LinkReleased, _definition.Steps.Count, detail: "all blocked internal links released");
            await VerifyReplicationAliveAsync();
        }

        public async Task<ScenarioExecutionReport> CaptureReportAsync() =>
            new()
            {
                Definition = _definition,
                MutationMode = _mutationMode,
                Timeline = _timeline.ToArray(),
                Snapshots = await LoadSnapshotsAsync()
            };

        public void AssertVerifiedState(ScenarioExecutionReport report)
        {
            foreach (var node in ScenarioCatalog.OrderedNodes)
                AssertNodeState(node, report.Snapshots[node], _definition.HealthyFinalStates[node]);

            var expectedTopology = _definition.HealthyCvTopology;
            var actualGroups = ScenarioCatalog.BuildActualChangeVectorGroups(report.Snapshots);
            Assert.Equal(expectedTopology.ExpectedExistingNodes, report.Snapshots.Where(x => x.Value.Exists).Select(x => x.Key).OrderBy(x => x).ToArray());
            Assert.Equal(expectedTopology.GroupKeys, actualGroups.Keys.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToArray());
            Assert.Equal(expectedTopology.DominantGroupKey, ScenarioCatalog.DetermineDominantGroupKey(actualGroups));


        }

        private async Task ConfigureHubDefinitionsAsync()
        {
            foreach (var node in ScenarioCatalog.OrderedNodes)
            {
                await _hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
                {
                    Name = GetHubDefinitionName(node),
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    WithFiltering = true,
                    MentorNode = _servers[node].ServerStore.NodeTag,
                    PinToMentorNode = true
                }));

                await _hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(
                    GetHubDefinitionName(node),
                    new ReplicationHubAccess
                    {
                        Name = GetHubAccessName(node),
                        CertificateBase64 = _pullCertificateBase64,
                        AllowedHubToSinkPaths = [_ticketWildcard],
                        AllowedSinkToHubPaths = [_ticketWildcard]
                    }));
            }
        }

        private async Task PrimeClusterAsync()
        {
            foreach (var node in ScenarioCatalog.OrderedNodes)
            {
                var docId = GetBootstrapDocumentId(node);
                await StoreUserDocumentAsync(_stores[node], docId, name: $"bootstrap-{node}");
                foreach (var otherClusterNode in ScenarioCatalog.OrderedNodes.Where(x => x != node))
                {
                    Assert.True(
                        _owner.WaitForDocument(_stores[otherClusterNode], docId, timeout: 60_000),
                        userMessage: $"Expected bootstrap document '{docId}' from {node} to arrive on {otherClusterNode}.");
                }
            }
        }

        private void EnsureInternalHandlersReady()
        {
            foreach (var sourceClusterNode in ScenarioCatalog.OrderedNodes)
            {
                var areHandlersReady = WaitForValue(() =>
                        _databases[sourceClusterNode].ReplicationLoader.OutgoingHandlers
                            .Count(replicationHandler => replicationHandler.Destination is InternalReplication) >= 2,
                    expectedVal: true,
                    timeout: 30_000);

                Assert.True(areHandlersReady, userMessage: $"Expected internal outgoing handlers to be ready on {sourceClusterNode}.");
            }
        }

        private async Task ExecuteStepAsync(int stepIndex, ScenarioStepDefinition step)
        {
            var stepNumber = stepIndex + 1;
            var blockedLinks = DetermineBlockedLinks(stepIndex);
            Record(
                CheckpointKind.StepStarted,
                stepNumber,
                detail: $"writer={step.WriterClusterNode}, " +
                        $"version={step.Version}, " +
                        $"mutation={_mutationMode}, " +
                        $"skipped=[{string.Join(",", step.SkippedTargets)}], " +
                        $"blocked=[{FormatLinks(blockedLinks)}]");

            using var maxItemsOverride = new TemporaryMaxItemsCountOverride(_databases[step.WriterClusterNode], maxItemsCount: 1);
            EnsureBlockedLinks(blockedLinks);

            await WriteScenarioStepAsync(stepIndex, step);
            Record(CheckpointKind.WriterSaved, stepNumber, detail: "writer transaction committed");

            await WaitForAllowedTargetsAsync(stepIndex, step);
            Record(CheckpointKind.AllowedTargetsObserved, stepNumber, detail: "non-skipped targets reached expected state");

            foreach (var skippedTargetClusterNode in step.SkippedTargets)
            {
                await InflateTargetAsync(stepIndex, source: step.WriterClusterNode, skippedTargetClusterNode);
                Record(
                    CheckpointKind.TargetInflated,
                    stepNumber,
                    detail: $"inflated target={skippedTargetClusterNode} " +
                            $"from writer={step.WriterClusterNode}");

                if (_mutationMode == BridgeTicketMutationMode.ModifyOnTarget)
                {
                    await ModifyInflatedTicketOnTargetAsync(stepIndex, skippedTargetClusterNode);
                    Record(
                        CheckpointKind.InflatedTicketModified,
                        stepNumber,
                        detail: $"mutated target={skippedTargetClusterNode} ticket={GetTicketDocumentId(stepIndex)}");
                }
            }

            if (stepIndex == _definition.Steps.Count - 1)
            {
                var writerLinks = step.SkippedTargets
                    .Select(targetClusterNode => (step.WriterClusterNode, targetClusterNode))
                    .ToArray();

                ReleaseBlockedLinks(writerLinks);
                Record(CheckpointKind.LinkReleased, stepNumber, detail: $"writer links released=[{FormatLinks(writerLinks)}]");

                await WaitForExpectedStatesAsync(_definition.HealthyFinalStates, stabilityWindow: TimeSpan.FromSeconds(1));
                Record(CheckpointKind.StepStateVerified, stepNumber, detail: "healthy final state verified after releasing writer links");

                return;
            }

            await WaitForExpectedStatesAsync(_definition.StatesAfterEachStep[stepIndex], stabilityWindow: TimeSpan.FromSeconds(1));
            Record(CheckpointKind.StepStateVerified, stepNumber, detail: "expected state verified under active link controls");
        }

        private async Task WriteScenarioStepAsync(int stepIndex, ScenarioStepDefinition step)
        {
            var expectedBeforeWriterNodeState = _definition.StatesBeforeEachStep[stepIndex][step.WriterClusterNode];
            using var session = _stores[step.WriterClusterNode].OpenAsyncSession();
            var doc = await session.LoadAsync<ScenarioDocument>(GetScenarioDocumentId());
            AssertWriterState(doc, expectedBeforeWriterNodeState, stepNumber: stepIndex + 1);

            await WriteWarmupDocumentsAsync(stepIndex, step.WriterClusterNode);
            Record(CheckpointKind.WarmupPrepared, stepNumber: stepIndex + 1, detail: $"warmup-count={WarmupDocumentCount}");

            await WriteScenarioDocumentAsync(step);
            await WriteTicketDocumentAsync(stepIndex);
        }

        private async Task WaitForAllowedTargetsAsync(int stepIndex, ScenarioStepDefinition step)
        {
            foreach (var node in ScenarioCatalog.OrderedNodes)
            {
                if (step.SkippedTargets.Contains(node))
                    continue;

                await WaitForNodeStateAsync(node, _definition.StatesAfterEachStep[stepIndex][node]);
            }
        }

        private async Task WaitForExpectedStatesAsync(IReadOnlyDictionary<ClusterNode, NodeState> expectedStates, TimeSpan stabilityWindow)
        {
            foreach (var node in ScenarioCatalog.OrderedNodes)
                await WaitForNodeStateAsync(node, expectedStates[node], stabilityWindow: stabilityWindow);
        }

        private void EnsureBlockedLinks(IReadOnlyList<(ClusterNode Source, ClusterNode Target)> blockedLinks)
        {
            foreach (var link in blockedLinks)
            {
                if (_retainedBlockers.ContainsKey(link))
                    continue;

                _retainedBlockers[link] = new InternalLinkBlocker(GetInternalHandler(link.Source, link.Target));
            }
        }

        private void ReleaseBlockedLinks(IReadOnlyList<(ClusterNode Source, ClusterNode Target)> links)
        {
            foreach (var link in links)
            {
                if (_retainedBlockers.TryGetValue(link, out var blocker) == false)
                    continue;

                blocker.Release();
            }
        }

        private DatabaseOutgoingReplicationHandler GetInternalHandler(ClusterNode source, ClusterNode target)
        {
            DatabaseOutgoingReplicationHandler handler = null;
            var hasHandler = WaitForValue(() =>
                {
                    handler = _databases[source].ReplicationLoader.OutgoingHandlers.SingleOrDefault(outgoing =>
                        outgoing.Destination is InternalReplication internalReplication &&
                        string.Equals(internalReplication.NodeTag, $"{target}", StringComparison.OrdinalIgnoreCase));
                    return handler != null;
                },
                expectedVal: true,
                timeout: 30_000);

            Assert.True(hasHandler, userMessage: $"Expected internal replication handler {source}->{target} to exist.");
            return handler;
        }

        private async Task InflateTargetAsync(int stepIndex, ClusterNode source, ClusterNode target)
        {
            var sinkStore = _owner.GetDocumentStore(new Options
            {
                AdminCertificate = _certificates.ServerCertificateForCommunication.Value,
                ClientCertificate = _certificates.ServerCertificateForCommunication.Value,
                ModifyDatabaseName = _ => GetBridgeDatabaseName(stepIndex, source, target)
            });

            try
            {
                var sourceTaskId = await CreateBridgeTaskAsync(
                    sinkStore,
                    GetBridgeConnectionName(stepIndex, source, target, role: "source"),
                    _servers[source].WebUrl,
                    new PullReplicationAsSink
                    {
                        ConnectionStringName = GetBridgeConnectionName(stepIndex, source, target, "source"),
                        Mode = PullReplicationMode.HubToSink,
                        CertificateWithPrivateKey = _pullCertificatePfxBase64,
                        HubName = GetHubDefinitionName(source),
                        AllowedHubToSinkPaths = [_ticketWildcard]
                    });

                Assert.True(
                    _owner.WaitForDocument(sinkStore, GetTicketDocumentId(stepIndex), timeout: 60_000),
                    userMessage: $"Expected ticket '{GetTicketDocumentId(stepIndex)}' to reach bridge sink for {source}->{target}.");

                var targetTaskId = await CreateBridgeTaskAsync(
                    sinkStore,
                    GetBridgeConnectionName(stepIndex, source, target, role: "target"),
                    _servers[target].WebUrl,
                    new PullReplicationAsSink
                    {
                        ConnectionStringName = GetBridgeConnectionName(stepIndex, source, target, role: "target"),
                        Mode = PullReplicationMode.SinkToHub,
                        CertificateWithPrivateKey = _pullCertificatePfxBase64,
                        HubName = GetHubDefinitionName(target),
                        AllowedSinkToHubPaths = [_ticketWildcard]
                    });

                Assert.True(
                    _owner.WaitForDocument(_stores[target], GetTicketDocumentId(stepIndex), timeout: 60_000),
                    userMessage: $"Expected ticket '{GetTicketDocumentId(stepIndex)}' to inflate target {target} from source {source}.");

                await sinkStore.Maintenance.SendAsync(new DeleteOngoingTaskOperation(targetTaskId, OngoingTaskType.PullReplicationAsSink));
                await sinkStore.Maintenance.SendAsync(new DeleteOngoingTaskOperation(sourceTaskId, OngoingTaskType.PullReplicationAsSink));
            }
            finally
            {
                sinkStore.Dispose();
            }
        }

        private async Task<long> CreateBridgeTaskAsync(DocumentStore sinkStore, string connectionName, string url, PullReplicationAsSink task)
        {
            await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = _databaseName,
                Name = connectionName,
                TopologyDiscoveryUrls = [url]
            }));

            var result = await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(task));
            return result.TaskId;
        }

        private async Task VerifyReplicationAliveAsync()
        {
            var markerWriterClusterNode = ScenarioCatalog.OrderedNodes.First(node => node != _definition.Steps[^1].WriterClusterNode);
            var markerTargetClusterNode = ScenarioCatalog.OrderedNodes.First(node => node != markerWriterClusterNode);
            var markerId = GetAliveMarkerDocumentId();

            await StoreUserDocumentAsync(_stores[markerWriterClusterNode], markerId, name: $"alive-{markerWriterClusterNode}");
            Assert.True(
                _owner.WaitForDocument(_stores[markerTargetClusterNode], markerId, timeout: 60_000),
                userMessage: $"Expected alive marker '{markerId}' from {markerWriterClusterNode} to reach {markerTargetClusterNode}.");

            Record(CheckpointKind.ReplicationAliveVerified, _definition.Steps.Count, detail: "post-gap replication is alive");
        }

        private async Task ModifyInflatedTicketOnTargetAsync(int stepIndex, ClusterNode target)
        {
            using var session = _stores[target].OpenAsyncSession();
            var ticketId = GetTicketDocumentId(stepIndex);
            var doc = await session.LoadAsync<User>(ticketId);
            Assert.NotNull(doc);

            doc.Name = $"{doc.Name ?? "ticket"}|mutated-on-{target}";
            await session.SaveChangesAsync();
        }

        private async Task WaitForNodeStateAsync(ClusterNode node, NodeState expectedState, TimeSpan? stabilityWindow = null)
        {
            var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(45);
            var interval = TimeSpan.FromMilliseconds(100);
            var sw = Stopwatch.StartNew();
            var stableSince = DateTime.UtcNow;
            var snapshot = default(ScenarioSnapshot);

            while (sw.Elapsed < timeout)
            {
                snapshot = await LoadSnapshotAsync(_stores[node]);
                if (MatchesState(snapshot, expectedState))
                {
                    if (stabilityWindow == null)
                        return;

                    if (DateTime.UtcNow - stableSince >= stabilityWindow.Value)
                        return;
                }
                else
                {
                    stableSince = DateTime.UtcNow;
                }

                await Task.Delay(interval);
            }

            throw new Xunit.Sdk.XunitException(
                $"Timed out waiting for node '{node}' to reach expected state '{FormatState(expectedState)}'. Actual snapshot: {FormatSnapshot(snapshot)}.");
        }

        private async Task<IReadOnlyDictionary<ClusterNode, ScenarioSnapshot>> LoadSnapshotsAsync()
        {
            var snapshots = new Dictionary<ClusterNode, ScenarioSnapshot>();
            foreach (var node in ScenarioCatalog.OrderedNodes)
                snapshots[node] = await LoadSnapshotAsync(_stores[node]);

            return snapshots;
        }

        private async Task<ScenarioSnapshot> LoadSnapshotAsync(IDocumentStore store)
        {
            using var session = store.OpenAsyncSession();
            var doc = await session.LoadAsync<ScenarioDocument>(GetScenarioDocumentId());
            if (doc == null)
                return new ScenarioSnapshot { Exists = false };

            return new ScenarioSnapshot
            {
                Exists = true,
                Version = doc.Version,
                WrittenBy = doc.WrittenBy,
                ChangeVector = session.Advanced.GetChangeVectorFor(doc)
            };
        }

        private static bool MatchesState(ScenarioSnapshot snapshot, NodeState expectedState)
        {
            if (snapshot.Exists != expectedState.Exists)
                return false;

            if (expectedState.Exists == false)
                return true;

            return snapshot.Version == expectedState.Version && snapshot.WrittenBy == expectedState.WrittenBy;
        }


        private IReadOnlyList<(ClusterNode Source, ClusterNode Target)> DetermineBlockedLinks(int stepIndex)
        {
            var expectedStates = _definition.StatesAfterEachStep[stepIndex];
            var blockedLinks = new List<(ClusterNode Source, ClusterNode Target)>();

            foreach (var source in ScenarioCatalog.OrderedNodes)
            {
                var sourceState = expectedStates[source];
                if (sourceState.Exists == false)
                    continue;

                foreach (var target in ScenarioCatalog.OrderedNodes)
                {
                    if (source == target)
                        continue;

                    var targetState = expectedStates[target];
                    if (ShouldBlockRepairingLink(sourceState, targetState))
                        blockedLinks.Add((source, target));
                }
            }

            return blockedLinks;
        }

        private static bool ShouldBlockRepairingLink(NodeState sourceState, NodeState targetState)
        {
            if (sourceState.Exists == false)
                return false;

            if (targetState.Exists == false)
                return true;

            return sourceState.LastActionOrdinal > targetState.LastActionOrdinal;
        }

        private static void AssertWriterState(ScenarioDocument document, NodeState expectedState, int stepNumber)
        {
            Assert.Equal(expectedState.Exists, document != null);
            if (expectedState.Exists == false)
                return;

            Assert.NotNull(document);
            Assert.Equal(expectedState.Version, document.Version);
            Assert.Equal(expectedState.WrittenBy, document.WrittenBy);
            Assert.True(stepNumber > 0);
        }

        private static void AssertNodeState(ClusterNode node, ScenarioSnapshot snapshot, NodeState expectedState)
        {
            Assert.Equal(expectedState.Exists, snapshot.Exists);
            if (expectedState.Exists == false)
                return;

            Assert.Equal(expectedState.Version, snapshot.Version);
            Assert.Equal(expectedState.WrittenBy, snapshot.WrittenBy);
            Assert.False(string.IsNullOrWhiteSpace(snapshot.ChangeVector), $"Expected non-empty change vector on node '{node}'.");
        }

        private void Record(CheckpointKind kind, int stepNumber, string detail)
        {
            _timeline.Add(new ScenarioCheckpoint
            {
                Kind = kind,
                StepNumber = stepNumber,
                Detail = detail
            });
        }

        private static async Task StoreUserDocumentAsync(IDocumentStore store, string id, string name)
        {
            using var session = store.OpenAsyncSession();
            await session.StoreAsync(new User { Name = name }, id);
            await session.SaveChangesAsync();
        }

        private static string FormatState(NodeState state) => state.Exists ? $"v{state.Version}/{state.WrittenBy!.Value}" : "missing";

        private static string FormatSnapshot(ScenarioSnapshot snapshot) => snapshot == null ? "<null>" : snapshot.Exists == false
                    ? "missing"
                    : $"v{snapshot.Version}/{snapshot.WrittenBy!.Value} cv={snapshot.ChangeVector}";

        private static string FormatLinks(IReadOnlyList<(ClusterNode Source, ClusterNode Target)> blockedLinks) =>
            blockedLinks.Count == 0
                ? "<none>"
                : string.Join(",", blockedLinks.Select(link => $"{link.Source}->{link.Target}"));

        private void ReleaseRetainedBlockers()
        {
            foreach (var blocker in _retainedBlockers.Values)
                blocker.Release();
        }

        private async Task WriteWarmupDocumentsAsync(int stepIndex, ClusterNode writer)
        {
            for (var ordinal = 1; ordinal <= WarmupDocumentCount; ordinal++)
            {
                await StoreUserDocumentAsync(
                    _stores[writer],
                    GetWarmupDocumentId(stepIndex, writer, ordinal),
                    name: $"warmup-{_definition.Id}-{writer}-{stepIndex + 1}-{ordinal}");
            }
        }

        private async Task WriteScenarioDocumentAsync(ScenarioStepDefinition step)
        {
            using var session = _stores[step.WriterClusterNode].OpenAsyncSession();
            var doc = await session.LoadAsync<ScenarioDocument>(GetScenarioDocumentId());
            if (doc == null)
            {
                doc = new ScenarioDocument();
                await session.StoreAsync(doc, GetScenarioDocumentId());
            }

            doc.Version = step.Version;
            doc.WrittenBy = step.WriterClusterNode;
            await session.SaveChangesAsync();
        }

        private async Task WriteTicketDocumentAsync(int stepIndex)
        {
            await StoreUserDocumentAsync(
                _stores[_definition.Steps[stepIndex].WriterClusterNode],
                GetTicketDocumentId(stepIndex),
                name: $"{_definition.Id}-ticket-step-{stepIndex + 1}");
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var blocker in _retainedBlockers.Values)
                blocker.Dispose();

            foreach (var store in _storesToDispose.Distinct().Reverse())
                store.Dispose();

            foreach (var certificate in _certificatesToDispose)
                certificate.Dispose();

            await Task.CompletedTask;
        }
    }

    private sealed class ScenarioDocument
    {
        public int Version { get; set; }

        public ClusterNode WrittenBy { get; set; }
    }

    internal sealed class ScenarioCheckpoint
    {
        public CheckpointKind Kind { get; init; }

        public int StepNumber { get; init; }

        public string Detail { get; init; }
    }

    private sealed class ScenarioExecutionReport
    {
        public ScenarioDefinition Definition { get; init; }

        public BridgeTicketMutationMode MutationMode { get; init; }

        public IReadOnlyList<ScenarioCheckpoint> Timeline { get; init; }

        public IReadOnlyDictionary<ClusterNode, ScenarioSnapshot> Snapshots { get; init; }
    }

    private sealed class InternalLinkBlocker : IDisposable
    {
        private readonly DatabaseOutgoingReplicationHandler _handler;
        private readonly ManualResetEventSlim _gate = new(initialState: false);
        private readonly Action _previous;

        public InternalLinkBlocker(DatabaseOutgoingReplicationHandler handler)
        {
            _handler = handler;
            _previous = handler.ForTestingPurposesOnly().OnDocumentSenderFetchNewItem;
            handler.ForTestingPurposesOnly().OnDocumentSenderFetchNewItem = Wait;
        }

        public void Release() => _gate.Set();

        public void Dispose()
        {
            _handler.ForTestingPurposesOnly().OnDocumentSenderFetchNewItem = _previous;
            _gate.Set();
            _gate.Dispose();
        }

        private void Wait()
        {
            _previous?.Invoke();
            _gate.Wait();
        }
    }

    private sealed class TemporaryMaxItemsCountOverride : IDisposable
    {
        private readonly DocumentDatabase _database;
        private readonly int? _originalMaxItemsCount;

        public TemporaryMaxItemsCountOverride(DocumentDatabase database, int maxItemsCount)
        {
            _database = database;
            _originalMaxItemsCount = database.Configuration.Replication.MaxItemsCount;
            database.Configuration.Replication.MaxItemsCount = maxItemsCount;
        }

        public void Dispose()
        {
            _database.Configuration.Replication.MaxItemsCount = _originalMaxItemsCount;
        }
    }

    private static class ScenarioFailureReportBuilder
    {
        public static string Build(ScenarioExecutionReport report, Exception exception)
        {
            var expectedStates = report.Definition.HealthyFinalStates;
            var expectedCvTopology = report.Definition.HealthyCvTopology;
            var actualGroups = ScenarioCatalog.BuildActualChangeVectorGroups(report.Snapshots);
            var dominantGroupKey = ScenarioCatalog.DetermineDominantGroupKey(actualGroups);
            var builder = new StringBuilder();
            builder.AppendLine("Filtered pull-replication cluster convergence scenario failed.");
            builder.AppendLine($"Scenario: {report.Definition.Id} ({report.Definition.Id})");
            builder.AppendLine($"Group: {report.Definition.Group}");
            builder.AppendLine($"Mutation mode: {report.MutationMode}");

            builder.AppendLine($"Expected dominant group: {expectedCvTopology.DominantGroupKey}");
            builder.AppendLine($"Actual dominant group: {dominantGroupKey ?? "<none>"}");
            builder.AppendLine($"Error: {exception.GetType().Name}: {exception.Message}");
            builder.AppendLine("Timeline:");
            foreach (var checkpoint in report.Timeline)
                builder.AppendLine($"  step={checkpoint.StepNumber} checkpoint={checkpoint.Kind} detail={checkpoint.Detail}");

            builder.AppendLine("Expected final states:");
            foreach (var node in ScenarioCatalog.OrderedNodes)
                builder.AppendLine($"  {node} => {FormatState(expectedStates[node])}");

            builder.AppendLine("Actual snapshots:");
            foreach (var node in ScenarioCatalog.OrderedNodes)
                builder.AppendLine($"  {node} => {FormatSnapshot(report.Snapshots[node])}");

            builder.AppendLine("Expected CV groups:");
            builder.AppendLine($"  groups=[{string.Join(", ", expectedCvTopology.GroupKeys)}]");

            builder.AppendLine("Actual CV groups:");
            if (actualGroups.Count == 0)
            {
                builder.AppendLine("  <none>");
            }
            else
            {
                foreach (var group in actualGroups.Values.OrderBy(x => x.GroupKey, StringComparer.OrdinalIgnoreCase))
                    builder.AppendLine($"  {group.GroupKey} => nodes=[{string.Join(",", group.Nodes)}] cv={group.NormalizedChangeVector}");
            }

            if (exception.StackTrace == null)
                return builder.ToString();

            builder.AppendLine("Stack trace:");
            builder.AppendLine(exception.StackTrace);

            return builder.ToString();
        }

        private static string FormatState(NodeState state)
        {
            return state.Exists
                ? $"v{state.Version}/{state.WrittenBy!.Value}"
                : "missing";
        }

        private static string FormatSnapshot(ScenarioSnapshot snapshot)
        {
            return snapshot.Exists == false
                ? "missing"
                : $"v{snapshot.Version}/{snapshot.WrittenBy!.Value} cv={snapshot.ChangeVector}";
        }
    }
}

public enum ScenarioId
{
    OnlyA,
    OnlyB,
    OnlyC,
    AbConsistentMissingC,
    AcConsistentMissingB,
    BcConsistentMissingA,
    AbInconsistentAWinsMissingC,
    AbInconsistentBWinsMissingC,
    AcInconsistentAWinsMissingB,
    AcInconsistentCWinsMissingB,
    BcInconsistentBWinsMissingA,
    BcInconsistentCWinsMissingA,
    AllThreeAWinsOneStaleB,
    AllThreeAWinsOneStaleC,
    AllThreeBWinsOneStaleA,
    AllThreeBWinsOneStaleC,
    AllThreeCWinsOneStaleA,
    AllThreeCWinsOneStaleB,
    AllThreeAWinsTwoStale,
    AllThreeBWinsTwoStale,
    AllThreeCWinsTwoStale,
    AllThreeAllDifferentAWins,
    AllThreeAllDifferentBWins,
    AllThreeAllDifferentCWins,
    AllThreeTieAbStaleC,
    AllThreeTieAcStaleB,
    AllThreeTieBcStaleA
}

internal enum ClusterNode
{
    A,
    B,
    C
}

internal enum ScenarioGroup
{
    SingleNode,
    TwoNodeConsistent,
    TwoNodeInconsistent,
    ThreeNodeOneStale,
    ThreeNodeTwoStale,
    ThreeNodeAllDifferent,
    ThreeNodeTie
}

internal enum ScenarioStepKind
{
    Create,
    Update
}

public enum BridgeTicketMutationMode
{
    None,
    ModifyOnTarget
}

internal enum CheckpointKind
{
    ClusterReady,
    StepStarted,
    WarmupPrepared,
    WriterSaved,
    AllowedTargetsObserved,
    TargetInflated,
    InflatedTicketModified,
    StepStateVerified,
    LinkReleased,
    FinalStateVerified,
    ReplicationAliveVerified
}

internal sealed class ScenarioDefinition
{
    public ScenarioId Id { get; init; }

    public ScenarioGroup Group { get; init; }

    public IReadOnlyList<ScenarioStepDefinition> Steps { get; init; }

    public IReadOnlyList<Dictionary<ClusterNode, NodeState>> StatesBeforeEachStep { get; init; }

    public IReadOnlyList<Dictionary<ClusterNode, NodeState>> StatesAfterEachStep { get; init; }

    public IReadOnlyDictionary<ClusterNode, NodeState> HealthyFinalStates { get; init; }

    public ExpectedCvTopology HealthyCvTopology { get; init; }
}

internal sealed class ScenarioStepDefinition
{
    public ScenarioStepDefinition(ScenarioStepKind kind, ClusterNode writerClusterNode, int version, params ClusterNode[] skippedTargets)
    {
        Kind = kind;
        WriterClusterNode = writerClusterNode;
        Version = version;
        SkippedTargets = skippedTargets?
            .Distinct()
            .OrderBy(x => x)
            .ToArray() ?? [];
    }

    public ScenarioStepKind Kind { get; }

    public ClusterNode WriterClusterNode { get; }

    public int Version { get; }

    public IReadOnlyList<ClusterNode> SkippedTargets { get; }
}

internal sealed class NodeState
{
    public bool Exists { get; init; }

    public int Version { get; init; }

    public ClusterNode? WrittenBy { get; init; }

    public int LastActionOrdinal { get; init; }

    public NodeState Clone() =>
        new()
        {
            Exists = Exists,
            Version = Version,
            WrittenBy = WrittenBy,
            LastActionOrdinal = LastActionOrdinal
        };
}

internal sealed class ExpectedCvTopology
{
    public IReadOnlyList<ClusterNode> ExpectedExistingNodes { get; init; }

    public IReadOnlyList<string> GroupKeys { get; init; }

    public string DominantGroupKey { get; init; }
}

internal sealed class ScenarioSnapshot
{
    public bool Exists { get; init; }

    public int Version { get; init; }

    public ClusterNode? WrittenBy { get; init; }

    public string ChangeVector { get; init; }
}

internal sealed class ChangeVectorGroup
{
    public string GroupKey { get; init; }

    public string NormalizedChangeVector { get; init; }

    public IReadOnlyList<ClusterNode> Nodes { get; init; }

    public Dictionary<string, long> ParsedChangeVector { get; init; }
}

internal sealed class ScenarioSimulation
{
    public ScenarioSimulation(
        IReadOnlyList<Dictionary<ClusterNode, NodeState>> statesBeforeEachStep,
        IReadOnlyList<Dictionary<ClusterNode, NodeState>> statesAfterEachStep)
    {
        StatesBeforeEachStep = statesBeforeEachStep;
        StatesAfterEachStep = statesAfterEachStep;
    }

    public IReadOnlyList<Dictionary<ClusterNode, NodeState>> StatesBeforeEachStep { get; }

    public IReadOnlyList<Dictionary<ClusterNode, NodeState>> StatesAfterEachStep { get; }
}

internal static class ScenarioCatalog
{
    private static readonly ClusterNode[] AllNodes =
    [
        ClusterNode.A,
        ClusterNode.B,
        ClusterNode.C
    ];

    private static string CreateGroupKey(IEnumerable<ClusterNode> nodes) => string.Concat(nodes.OrderBy(x => x));
    private static ScenarioStepDefinition CreateStep(ScenarioStepKind kind, ClusterNode writerClusterNode, int version, params ClusterNode[] skippedTargetsClusterNodes) => new(kind, writerClusterNode, version, skippedTargetsClusterNodes);
    public static readonly Lazy<IReadOnlyDictionary<ScenarioId, ScenarioDefinition>> Definitions = new(CreateDefinitions);
    public static IReadOnlyList<ClusterNode> OrderedNodes => AllNodes;

    private static Dictionary<ScenarioId, ScenarioDefinition> CreateDefinitions() =>
        new()
        {
            [ScenarioId.OnlyA] = CreateScenario(
                ScenarioId.OnlyA,
                ScenarioGroup.SingleNode,
                expectedWinnerVersion: 1,
                expectedWinnerWriter: ClusterNode.A,
                CreateStep(ScenarioStepKind.Create, ClusterNode.A, version: 1, ClusterNode.B, ClusterNode.C)),

            [ScenarioId.OnlyB] = CreateScenario(
                ScenarioId.OnlyB,
                ScenarioGroup.SingleNode,
                expectedWinnerVersion: 1,
                expectedWinnerWriter: ClusterNode.B,
                CreateStep(ScenarioStepKind.Create, ClusterNode.B, version: 1, ClusterNode.A, ClusterNode.C)),

            [ScenarioId.OnlyC] = CreateScenario(
                ScenarioId.OnlyC,
                ScenarioGroup.SingleNode,
                expectedWinnerVersion: 1,
                expectedWinnerWriter: ClusterNode.C,
                CreateStep(ScenarioStepKind.Create, ClusterNode.C, version: 1, ClusterNode.A, ClusterNode.B)),

            [ScenarioId.AbConsistentMissingC] = CreateScenario(
                ScenarioId.AbConsistentMissingC,
                ScenarioGroup.TwoNodeConsistent,
                expectedWinnerVersion: 1,
                expectedWinnerWriter: ClusterNode.A,
                CreateStep(ScenarioStepKind.Create, ClusterNode.A, version: 1, ClusterNode.C)),

            [ScenarioId.AcConsistentMissingB] = CreateScenario(
                ScenarioId.AcConsistentMissingB,
                ScenarioGroup.TwoNodeConsistent,
                expectedWinnerVersion: 1,
                expectedWinnerWriter: ClusterNode.A,
                CreateStep(ScenarioStepKind.Create, ClusterNode.A, version: 1, ClusterNode.B)),

            [ScenarioId.BcConsistentMissingA] = CreateScenario(
                ScenarioId.BcConsistentMissingA,
                ScenarioGroup.TwoNodeConsistent,
                expectedWinnerVersion: 1,
                expectedWinnerWriter: ClusterNode.B,
                CreateStep(ScenarioStepKind.Create, ClusterNode.B, version: 1, ClusterNode.A)),

            [ScenarioId.AbInconsistentAWinsMissingC] = CreateScenario(
                ScenarioId.AbInconsistentAWinsMissingC,
                ScenarioGroup.TwoNodeInconsistent,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.A,
                CreateStep(ScenarioStepKind.Create, ClusterNode.B, version: 1, ClusterNode.C),
                CreateStep(ScenarioStepKind.Update, ClusterNode.A, version: 2, ClusterNode.B, ClusterNode.C)),

            [ScenarioId.AbInconsistentBWinsMissingC] = CreateScenario(
                ScenarioId.AbInconsistentBWinsMissingC,
                ScenarioGroup.TwoNodeInconsistent,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.B,
                CreateStep(ScenarioStepKind.Create, ClusterNode.A, version: 1, ClusterNode.C),
                CreateStep(ScenarioStepKind.Update, ClusterNode.B, version: 2, ClusterNode.A, ClusterNode.C)),

            [ScenarioId.AcInconsistentAWinsMissingB] = CreateScenario(
                ScenarioId.AcInconsistentAWinsMissingB,
                ScenarioGroup.TwoNodeInconsistent,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.A,
                CreateStep(ScenarioStepKind.Create, ClusterNode.C, version: 1, ClusterNode.B),
                CreateStep(ScenarioStepKind.Update, ClusterNode.A, version: 2, ClusterNode.B, ClusterNode.C)),

            [ScenarioId.AcInconsistentCWinsMissingB] = CreateScenario(
                ScenarioId.AcInconsistentCWinsMissingB,
                ScenarioGroup.TwoNodeInconsistent,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.C,
                CreateStep(ScenarioStepKind.Create, ClusterNode.A, version: 1, ClusterNode.B),
                CreateStep(ScenarioStepKind.Update, ClusterNode.C, version: 2, ClusterNode.A, ClusterNode.B)),

            [ScenarioId.BcInconsistentBWinsMissingA] = CreateScenario(
                ScenarioId.BcInconsistentBWinsMissingA,
                ScenarioGroup.TwoNodeInconsistent,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.B,
                CreateStep(ScenarioStepKind.Create, ClusterNode.C, version: 1, ClusterNode.A),
                CreateStep(ScenarioStepKind.Update, ClusterNode.B, version: 2, ClusterNode.A, ClusterNode.C)),

            [ScenarioId.BcInconsistentCWinsMissingA] = CreateScenario(
                ScenarioId.BcInconsistentCWinsMissingA,
                ScenarioGroup.TwoNodeInconsistent,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.C,
                CreateStep(ScenarioStepKind.Create, ClusterNode.B, version: 1, ClusterNode.A),
                CreateStep(ScenarioStepKind.Update, ClusterNode.C, version: 2, ClusterNode.A, ClusterNode.B)),

            [ScenarioId.AllThreeAWinsOneStaleB] = CreateScenario(
                ScenarioId.AllThreeAWinsOneStaleB,
                ScenarioGroup.ThreeNodeOneStale,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.A,
                CreateStep(ScenarioStepKind.Create, ClusterNode.C, version: 1),
                CreateStep(ScenarioStepKind.Update, ClusterNode.A, version: 2, ClusterNode.B)),

            [ScenarioId.AllThreeAWinsOneStaleC] = CreateScenario(
                ScenarioId.AllThreeAWinsOneStaleC,
                ScenarioGroup.ThreeNodeOneStale,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.A,
                CreateStep(ScenarioStepKind.Create, ClusterNode.B, version: 1),
                CreateStep(ScenarioStepKind.Update, ClusterNode.A, version: 2, ClusterNode.C)),

            [ScenarioId.AllThreeBWinsOneStaleA] = CreateScenario(
                ScenarioId.AllThreeBWinsOneStaleA,
                ScenarioGroup.ThreeNodeOneStale,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.B,
                CreateStep(ScenarioStepKind.Create, ClusterNode.C, version: 1),
                CreateStep(ScenarioStepKind.Update, ClusterNode.B, version: 2, ClusterNode.A)),

            [ScenarioId.AllThreeBWinsOneStaleC] = CreateScenario(
                ScenarioId.AllThreeBWinsOneStaleC,
                ScenarioGroup.ThreeNodeOneStale,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.B,
                CreateStep(ScenarioStepKind.Create, ClusterNode.A, version: 1),
                CreateStep(ScenarioStepKind.Update, ClusterNode.B, version: 2, ClusterNode.C)),

            [ScenarioId.AllThreeCWinsOneStaleA] = CreateScenario(
                ScenarioId.AllThreeCWinsOneStaleA,
                ScenarioGroup.ThreeNodeOneStale,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.C,
                CreateStep(ScenarioStepKind.Create, ClusterNode.B, version: 1),
                CreateStep(ScenarioStepKind.Update, ClusterNode.C, version: 2, ClusterNode.A)),

            [ScenarioId.AllThreeCWinsOneStaleB] = CreateScenario(
                ScenarioId.AllThreeCWinsOneStaleB,
                ScenarioGroup.ThreeNodeOneStale,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.C,
                CreateStep(ScenarioStepKind.Create, ClusterNode.A, version: 1),
                CreateStep(ScenarioStepKind.Update, ClusterNode.C, version: 2, ClusterNode.B)),

            [ScenarioId.AllThreeAWinsTwoStale] = CreateScenario(
                ScenarioId.AllThreeAWinsTwoStale,
                ScenarioGroup.ThreeNodeTwoStale,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.A,
                CreateStep(ScenarioStepKind.Create, ClusterNode.C, version: 1),
                CreateStep(ScenarioStepKind.Update, ClusterNode.A, version: 2, ClusterNode.B, ClusterNode.C)),

            [ScenarioId.AllThreeBWinsTwoStale] = CreateScenario(
                ScenarioId.AllThreeBWinsTwoStale,
                ScenarioGroup.ThreeNodeTwoStale,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.B,
                CreateStep(ScenarioStepKind.Create, ClusterNode.A, version: 1),
                CreateStep(ScenarioStepKind.Update, ClusterNode.B, version: 2, ClusterNode.A, ClusterNode.C)),

            [ScenarioId.AllThreeCWinsTwoStale] = CreateScenario(
                ScenarioId.AllThreeCWinsTwoStale,
                ScenarioGroup.ThreeNodeTwoStale,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.C,
                CreateStep(ScenarioStepKind.Create, ClusterNode.B, version: 1),
                CreateStep(ScenarioStepKind.Update, ClusterNode.C, version: 2, ClusterNode.A, ClusterNode.B)),

            [ScenarioId.AllThreeAllDifferentAWins] = CreateScenario(
                ScenarioId.AllThreeAllDifferentAWins,
                ScenarioGroup.ThreeNodeAllDifferent,
                expectedWinnerVersion: 3,
                expectedWinnerWriter: ClusterNode.A,
                CreateStep(ScenarioStepKind.Create, ClusterNode.C, version: 1),
                CreateStep(ScenarioStepKind.Update, ClusterNode.B, version: 2, ClusterNode.C),
                CreateStep(ScenarioStepKind.Update, ClusterNode.A, version: 3, ClusterNode.B, ClusterNode.C)),

            [ScenarioId.AllThreeAllDifferentBWins] = CreateScenario(
                ScenarioId.AllThreeAllDifferentBWins,
                ScenarioGroup.ThreeNodeAllDifferent,
                expectedWinnerVersion: 3,
                expectedWinnerWriter: ClusterNode.B,
                CreateStep(ScenarioStepKind.Create, ClusterNode.A, version: 1),
                CreateStep(ScenarioStepKind.Update, ClusterNode.C, version: 2, ClusterNode.A),
                CreateStep(ScenarioStepKind.Update, ClusterNode.B, version: 3, ClusterNode.A, ClusterNode.C)),

            [ScenarioId.AllThreeAllDifferentCWins] = CreateScenario(
                ScenarioId.AllThreeAllDifferentCWins,
                ScenarioGroup.ThreeNodeAllDifferent,
                expectedWinnerVersion: 3,
                expectedWinnerWriter: ClusterNode.C,
                CreateStep(ScenarioStepKind.Create, ClusterNode.B, version: 1),
                CreateStep(ScenarioStepKind.Update, ClusterNode.A, version: 2, ClusterNode.B),
                CreateStep(ScenarioStepKind.Update, ClusterNode.C, version: 3, ClusterNode.A, ClusterNode.B)),

            [ScenarioId.AllThreeTieAbStaleC] = CreateScenario(
                ScenarioId.AllThreeTieAbStaleC,
                ScenarioGroup.ThreeNodeTie,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.A,
                CreateStep(ScenarioStepKind.Create, ClusterNode.C, version: 1),
                CreateStep(ScenarioStepKind.Update, ClusterNode.A, version: 2, ClusterNode.C)),

            [ScenarioId.AllThreeTieAcStaleB] = CreateScenario(
                ScenarioId.AllThreeTieAcStaleB,
                ScenarioGroup.ThreeNodeTie,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.A,
                CreateStep(ScenarioStepKind.Create, ClusterNode.B, version: 1),
                CreateStep(ScenarioStepKind.Update, ClusterNode.A, version: 2, ClusterNode.B)),

            [ScenarioId.AllThreeTieBcStaleA] = CreateScenario(
                ScenarioId.AllThreeTieBcStaleA,
                ScenarioGroup.ThreeNodeTie,
                expectedWinnerVersion: 2,
                expectedWinnerWriter: ClusterNode.B,
                CreateStep(ScenarioStepKind.Create, ClusterNode.A, version: 1),
                CreateStep(ScenarioStepKind.Update, ClusterNode.B, version: 2, ClusterNode.A))
        };

    private static ScenarioDefinition CreateScenario(
        ScenarioId id,
        ScenarioGroup group,
        int expectedWinnerVersion,
        ClusterNode expectedWinnerWriter,
        params ScenarioStepDefinition[] scenarioStepDefinitions)
    {
        var simulation = SimulateScenarioStates(scenarioStepDefinitions);
        return new ScenarioDefinition
        {
            Id = id,
            Group = group,
            Steps = scenarioStepDefinitions.ToList(),
            StatesBeforeEachStep = simulation.StatesBeforeEachStep,
            StatesAfterEachStep = simulation.StatesAfterEachStep,
            HealthyFinalStates = BuildHealthyFinalStates(expectedWinnerVersion, expectedWinnerWriter, scenarioStepDefinitions.Length),
            HealthyCvTopology = BuildHealthyCvTopology()
        };
    }

    private static ScenarioSimulation SimulateScenarioStates(IReadOnlyList<ScenarioStepDefinition> steps)
    {
        var currentStates = AllNodes.ToDictionary(node => node, _ => new NodeState(), EqualityComparer<ClusterNode>.Default);
        var beforeStates = new List<Dictionary<ClusterNode, NodeState>>(steps.Count);
        var afterStates = new List<Dictionary<ClusterNode, NodeState>>(steps.Count);

        for (var i = 0; i < steps.Count; i++)
        {
            var step = steps[i];
            beforeStates.Add(CloneStates(currentStates));

            var writerState = currentStates[step.WriterClusterNode];
            switch (step.Kind)
            {
                case ScenarioStepKind.Create when writerState.Exists:
                    throw new InvalidOperationException($"Scenario '{step.WriterClusterNode}' create step '{i + 1}' expected a missing document on the writer.");
                case ScenarioStepKind.Update when writerState.Exists == false:
                    throw new InvalidOperationException($"Scenario '{step.WriterClusterNode}' update step '{i + 1}' expected the document to already exist on the writer.");
            }

            var replicatedState = new NodeState
            {
                Exists = true,
                Version = step.Version,
                WrittenBy = step.WriterClusterNode,
                LastActionOrdinal = i + 1
            };

            currentStates[step.WriterClusterNode] = replicatedState.Clone();

            foreach (var node in AllNodes)
            {
                if (node == step.WriterClusterNode)
                    continue;

                if (step.SkippedTargets.Contains(node))
                    continue;

                currentStates[node] = replicatedState.Clone();
            }

            afterStates.Add(CloneStates(currentStates));
        }

        return new ScenarioSimulation(beforeStates, afterStates);
    }

    private static Dictionary<ClusterNode, NodeState> CloneStates(IReadOnlyDictionary<ClusterNode, NodeState> states) =>
        states.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.Clone(), EqualityComparer<ClusterNode>.Default);

    private static Dictionary<ClusterNode, NodeState> BuildHealthyFinalStates(int expectedWinnerVersion, ClusterNode expectedWinnerWriter, int actionOrdinal) =>
        AllNodes.ToDictionary(
            node => node,
            _ => new NodeState
            {
                Exists = true,
                Version = expectedWinnerVersion,
                WrittenBy = expectedWinnerWriter,
                LastActionOrdinal = actionOrdinal
            },
            EqualityComparer<ClusterNode>.Default);

    private static ExpectedCvTopology BuildHealthyCvTopology()
    {
        var nodes = AllNodes.OrderBy(x => x).ToArray();
        var groupKey = CreateGroupKey(nodes);

        return new ExpectedCvTopology
        {
            ExpectedExistingNodes = nodes,
            GroupKeys = [groupKey],
            DominantGroupKey = groupKey
        };
    }

    public static IReadOnlyDictionary<string, ChangeVectorGroup> BuildActualChangeVectorGroups(
        IReadOnlyDictionary<ClusterNode, ScenarioSnapshot> snapshots)
    {
        return snapshots
            .Where(x => x.Value.Exists)
            .GroupBy(x => NormalizeChangeVector(x.Value.ChangeVector), StringComparer.OrdinalIgnoreCase)
            .Select(group =>
            {
                var nodes = group.Select(x => x.Key).OrderBy(clusterNode => clusterNode).ToArray();
                var groupKey = CreateGroupKey(nodes);
                return new KeyValuePair<string, ChangeVectorGroup>(
                    groupKey,
                    new ChangeVectorGroup
                    {
                        GroupKey = groupKey,
                        NormalizedChangeVector = group.Key,
                        Nodes = nodes,
                        ParsedChangeVector = ParseChangeVector(group.First().Value.ChangeVector)
                    });
            })
            .ToDictionary(x => x.Key, x => x.Value, StringComparer.OrdinalIgnoreCase);
    }

    public static string DetermineDominantGroupKey(IReadOnlyDictionary<string, ChangeVectorGroup> groups)
    {
        if (groups.Count == 0)
            return null;

        if (groups.Count == 1)
            return groups.Keys.Single();

        string dominantGroupKey = null;
        foreach (var candidate in groups.Values)
        {
            var isDominatesAllOthers = groups.Values
                .Where(group => ReferenceEquals(group, candidate) == false)
                .All(otherChangeVectorGroup => Dominates(left: candidate.ParsedChangeVector, right: otherChangeVectorGroup.ParsedChangeVector));

            if (isDominatesAllOthers == false)
                continue;

            if (dominantGroupKey != null)
                return null;

            dominantGroupKey = candidate.GroupKey;
        }

        return dominantGroupKey;
    }

    private static bool Dominates(Dictionary<string, long> left, Dictionary<string, long> right)
    {
        var hasStrictlyGreaterEntry = false;
        foreach (var tag in left.Keys.Concat(right.Keys).Distinct(StringComparer.OrdinalIgnoreCase))
        {
            left.TryGetValue(tag, out var leftEtag);
            right.TryGetValue(tag, out var rightEtag);

            if (leftEtag < rightEtag)
                return false;

            if (leftEtag > rightEtag)
                hasStrictlyGreaterEntry = true;
        }

        return hasStrictlyGreaterEntry;
    }

    private static string NormalizeChangeVector(string changeVector)
    {
        if (string.IsNullOrWhiteSpace(changeVector))
            return string.Empty;

        return string.Join("|", ParseChangeVector(changeVector)
            .OrderBy(x => x.Key, StringComparer.OrdinalIgnoreCase)
            .Select(x => $"{x.Key}:{x.Value}"));
    }

    private static Dictionary<string, long> ParseChangeVector(string changeVector)
    {
        var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(changeVector))
            return result;

        var entries = changeVector.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        foreach (var entry in entries)
        {
            var colonIndex = entry.IndexOf(':');
            if (colonIndex <= 0)
                continue;

            var dashIndex = entry.IndexOf('-', colonIndex + 1);
            var tag = entry.Substring(0, colonIndex).Trim();
            var etagText = dashIndex > colonIndex
                ? entry.Substring(colonIndex + 1, dashIndex - colonIndex - 1)
                : entry[(colonIndex + 1)..];

            if (long.TryParse(etagText, out var etag) == false)
                continue;

            result[tag] = etag;
        }

        return result;
    }
}
