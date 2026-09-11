using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Sparrow.Platform;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.ExtensionPoints
{
    public class DatabaseEventExecClusterTests : ClusterTestBase
    {
        public DatabaseEventExecClusterTests(ITestOutputHelper output) : base(output)
        {
        }

        private static string EchoNameScript => PlatformDetails.RunningOnPosix
            ? "#!/bin/bash\necho \"$3\" >> \"$1\"\nexit 0\n"
            : @"
param([string]$outputPath, [string]$dbName)
Add-Content $outputPath $dbName
exit 0";

        private static (string ScriptFile, string[] OutputFiles) NewTempPaths(int numberOfNodes)
        {
            string scriptExtension = PlatformDetails.RunningOnPosix ? ".sh" : ".ps1";
            return (Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), scriptExtension)),
                Enumerable.Range(0, numberOfNodes)
                    .Select(_ => Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".txt")))
                    .ToArray());
        }

        /// <summary>
        /// One settings dictionary per node, each pointing the hook at its own output file so we can tell
        /// which node ran it. NOTE: when customSettingsList is supplied, DefaultClusterSettings and the
        /// election timeout are NOT merged in for us - each dictionary must carry them itself.
        /// </summary>
        private List<IDictionary<string, string>> BuildPerNodeSettings(string scriptFile, string[] outputFiles)
        {
            List<IDictionary<string, string>> customSettingsList = new List<IDictionary<string, string>>();

            for (int i = 0; i < outputFiles.Length; i++)
            {
                Dictionary<string, string> settings = new Dictionary<string, string>(DefaultClusterSettings)
                {
                    [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = "300"
                };

                if (PlatformDetails.RunningOnPosix)
                {
                    settings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExec)] = "bash";
                    settings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExecArguments)] = $"{scriptFile} {outputFiles[i]}";
                }
                else
                {
                    settings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExec)] = "powershell";
                    settings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExecArguments)] = $"-NoProfile {scriptFile} {outputFiles[i]}";
                }

                customSettingsList.Add(settings);
            }

            return customSettingsList;
        }

        private static void CleanupTempPaths(string scriptFile, string[] outputFiles)
        {
            foreach (string path in new[] { scriptFile }.Concat(outputFiles))
            {
                try
                {
                    if (File.Exists(path))
                        File.Delete(path);
                }
                catch (IOException)
                {
                    // a still running script may hold the file on Windows
                }
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_FiresOnceOnTheLeaderOnly()
        {
            const int numberOfNodes = 3;
            (string scriptFile, string[] outputFiles) = NewTempPaths(numberOfNodes);

            try
            {
                File.WriteAllText(scriptFile, EchoNameScript);

                (List<RavenServer> nodes, RavenServer leader) = await CreateRaftCluster(
                    numberOfNodes, leaderIndex: 0, customSettingsList: BuildPerNodeSettings(scriptFile, outputFiles));

                int leaderIndex = nodes.IndexOf(leader);
                Assert.True(leaderIndex >= 0, "Could not identify the leader among the cluster nodes.");

                using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = numberOfNodes }))
                {
                    string databaseName = store.Database;

                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true));

                    bool leaderRanTheHook = await WaitForValueAsync(() => File.Exists(outputFiles[leaderIndex]), true, timeout: 30_000);
                    Assert.True(leaderRanTheHook, "The leader did not run the deletion hook.");

                    // give any other node that was going to run it a fair chance to do so
                    await Task.Delay(TimeSpan.FromSeconds(3));

                    string[] whoRan = outputFiles
                        .Select((file, index) => (file, index))
                        .Where(x => File.Exists(x.file))
                        .Select(x => $"node {nodes[x.index].ServerStore.NodeTag}")
                        .ToArray();

                    Assert.Equal(new[] { $"node {leader.ServerStore.NodeTag}" }, whoRan);

                    // and exactly one invocation on that node, not one per replica
                    Assert.Equal(new[] { databaseName },
                        File.ReadAllLines(outputFiles[leaderIndex]).Where(x => string.IsNullOrWhiteSpace(x) == false).ToArray());
                }
            }
            finally
            {
                CleanupTempPaths(scriptFile, outputFiles);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_WhenRemovingOneNodeAndOthersRemain_HookDoesNotFire()
        {
            const int numberOfNodes = 3;
            (string scriptFile, string[] outputFiles) = NewTempPaths(numberOfNodes);

            try
            {
                File.WriteAllText(scriptFile, EchoNameScript);

                (List<RavenServer> nodes, RavenServer leader) = await CreateRaftCluster(
                    numberOfNodes, leaderIndex: 0, customSettingsList: BuildPerNodeSettings(scriptFile, outputFiles));

                using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = numberOfNodes }))
                {
                    string databaseName = store.Database;

                    // shrink the replication factor rather than delete the database. Two nodes still hold a
                    // copy, so this is not a whole-database deletion and the hook must stay silent.
                    RavenServer nodeToRemove = nodes.First(x => x != leader);
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true,
                        fromNode: nodeToRemove.ServerStore.NodeTag, timeToWaitForConfirmation: TimeSpan.FromSeconds(30)));

                    await Task.Delay(TimeSpan.FromSeconds(5));

                    string[] whoRan = outputFiles
                        .Select((file, index) => (file, index))
                        .Where(x => File.Exists(x.file))
                        .Select(x => $"node {nodes[x.index].ServerStore.NodeTag}")
                        .ToArray();

                    Assert.Empty(whoRan);

                    // the database itself is still there, just on fewer nodes
                    string[] names = await store.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, 1024));
                    Assert.Contains(databaseName, names);
                }
            }
            finally
            {
                CleanupTempPaths(scriptFile, outputFiles);
            }
        }
    }
}
