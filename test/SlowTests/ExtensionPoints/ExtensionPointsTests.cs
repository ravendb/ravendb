using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Config;
using Raven.Server.Utils;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.ExtensionPoints
{
    public class ExtensionPointsTests : RavenTestBase
    {
        private const string SystemDbName = "System";

        [Fact(Skip = "https://github.com/dotnet/corefx/issues/30691")]
        public async Task OnDirectoryInitializeInMemoryTest()
        {
            string script;
            IDictionary<string, string> customSettings = new ConcurrentDictionary<string, string>();

            var scriptFile = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            var outputFile = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".txt"));

            if (PlatformDetails.RunningOnPosix)
            {
                customSettings[RavenConfiguration.GetKey(x => x.Storage.OnDirectoryInitializeExec)] = "bash";
                customSettings[RavenConfiguration.GetKey(x => x.Storage.OnDirectoryInitializeExecArguments)] = $"{scriptFile} {outputFile}";

                script = "#!/bin/bash\r\necho \"$2 $3 $4 $5 $6\" >> $1";
                File.WriteAllText(scriptFile, script);
                Process.Start("chmod", $"700 {scriptFile}");
            }
            else
            {
                customSettings[RavenConfiguration.GetKey(x => x.Storage.OnDirectoryInitializeExec)] = "powershell";
                customSettings[RavenConfiguration.GetKey(x => x.Storage.OnDirectoryInitializeExecArguments)] = $"{scriptFile} {outputFile}";

                script = @"
param([string]$userArg ,[string]$type, [string]$name, [string]$dataPath, [string]$tempPath, [string]$journalPath)
Add-Content $userArg ""$type $name $dataPath $tempPath $journalPath\r\n""
exit 0";
                File.WriteAllText(scriptFile, script);
            }

            UseNewLocalServer(customSettings: customSettings);

            // Creating dummy storage env options, so we can tell all the different paths
            using (var options = StorageEnvironmentOptions.CreateMemoryOnly())
            {
                using (var store = GetDocumentStore())
                {
                    store.Maintenance.Send(new CreateSampleDataOperation());

                    // the database loads after all indexes are loaded
                    var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                    var lines = File.ReadAllLines(outputFile);
                    Assert.True(lines.Length == 6);
                    Assert.True(lines[0].Contains($"{DirectoryExecUtils.EnvironmentType.System} {SystemDbName} {options.BasePath} {options.TempPath} {options.JournalPath}"));
                    Assert.True(lines[1].Contains($"{DirectoryExecUtils.EnvironmentType.Configuration} {store.Database} {options.BasePath} {options.TempPath} {options.JournalPath}"));
                    Assert.True(lines[2].Contains($"{DirectoryExecUtils.EnvironmentType.Database} {store.Database} {options.BasePath} {options.TempPath} {options.JournalPath}"));

                    var indexes = documentDatabase.IndexStore.GetIndexes().ToArray();

                    Assert.True(indexes.Length == 3);

                    // The indexes order in the IndexStore don't match the order of storage env creation and we need a one-to-one match.
                    var matches = lines.ToList().GetRange(3, 3);

                    foreach (var index in indexes)
                    {
                        var expected = $"{DirectoryExecUtils.EnvironmentType.Index} {store.Database} {index._environment.Options.BasePath} {index._environment.Options.TempPath} {index._environment.Options.JournalPath}";
                        var indexToRemove = matches.FindIndex(str => str.Contains(expected));
                        if (indexToRemove != -1)
                            matches.RemoveAt(indexToRemove);
                    }

                    Assert.Equal(0, matches.Count);
                }
            }
        }

        [Fact(Skip = "https://github.com/dotnet/corefx/issues/30691")]
        public async Task OnDirectoryInitializePersistedTest()
        {
            string script;
            IDictionary<string, string> customSettings = new ConcurrentDictionary<string, string>();

            var scriptFile = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            var outputFile = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".txt"));

            if (PlatformDetails.RunningOnPosix)
            {
                customSettings[RavenConfiguration.GetKey(x => x.Storage.OnDirectoryInitializeExec)] = "bash";
                customSettings[RavenConfiguration.GetKey(x => x.Storage.OnDirectoryInitializeExecArguments)] = $"{scriptFile} {outputFile}";

                script = "#!/bin/bash\r\necho \"$2 $3 $4 $5 $6\" >> $1";
                File.WriteAllText(scriptFile, script);
                Process.Start("chmod", $"700 {scriptFile}");
            }
            else
            {
                customSettings[RavenConfiguration.GetKey(x => x.Storage.OnDirectoryInitializeExec)] = "powershell";
                customSettings[RavenConfiguration.GetKey(x => x.Storage.OnDirectoryInitializeExecArguments)] = $"{scriptFile} {outputFile}";

                script = @"
param([string]$userArg ,[string]$type, [string]$name, [string]$dataPath, [string]$tempPath, [string]$journalPath)
Add-Content $userArg ""$type $name $dataPath $tempPath $journalPath\r\n""
exit 0";
                File.WriteAllText(scriptFile, script);
            }

            UseNewLocalServer(customSettings: customSettings, runInMemory: false);
            var basePath = NewDataPath();

            using (var store = GetDocumentStore(new Options
            {
                Path = basePath
            }))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                // The database loads after all indexes are loaded
                var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                var lines = File.ReadAllLines(outputFile);
                Assert.True(lines.Length == 6);

                var systemEnvOptions = Server.ServerStore._env.Options;
                var configEnvOptions = documentDatabase.ConfigurationStorage.Environment.Options;
                var docsEnvOptions = documentDatabase.DocumentsStorage.Environment.Options;

                Assert.True(lines[0].Contains($"{DirectoryExecUtils.EnvironmentType.System} {SystemDbName} {systemEnvOptions.BasePath} {systemEnvOptions.TempPath} {systemEnvOptions.JournalPath}"));
                Assert.True(lines[1].Contains($"{DirectoryExecUtils.EnvironmentType.Configuration} {store.Database} {configEnvOptions.BasePath} {configEnvOptions.TempPath} {configEnvOptions.JournalPath}"));
                Assert.True(lines[2].Contains($"{DirectoryExecUtils.EnvironmentType.Database} {store.Database} {docsEnvOptions.BasePath} {docsEnvOptions.TempPath} {docsEnvOptions.JournalPath}"));

                var indexes = documentDatabase.IndexStore.GetIndexes().ToArray();

                Assert.True(indexes.Length == 3);

                // The indexes order in the IndexStore don't match the order of storage env creation and we need a one-to-one match.
                var matches = lines.ToList().GetRange(3, 3);

                foreach (var index in indexes)
                {
                    var expected = $"{DirectoryExecUtils.EnvironmentType.Index} {store.Database} {index._environment.Options.BasePath} {index._environment.Options.TempPath} {index._environment.Options.JournalPath}";
                    var indexToRemove = matches.FindIndex(str => str.Contains(expected));
                    if (indexToRemove != -1)
                        matches.RemoveAt(indexToRemove);
                }

                Assert.Equal(0, matches.Count);
            }
        }
    }
}

