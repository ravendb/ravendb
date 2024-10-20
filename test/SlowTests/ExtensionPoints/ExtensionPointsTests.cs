﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Utils;
using Sparrow.Platform;
using Sparrow.Utils;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.ExtensionPoints
{
    public class ExtensionPointsTests : RavenTestBase
    {
        public ExtensionPointsTests(ITestOutputHelper output) : base(output)
        {
        }

        private const string SystemDbName = "System";

        [Fact]
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
            using (var options = StorageEnvironmentOptions.CreateMemoryOnlyForTests())
            {
                using (var store = GetDocumentStore())
                {
                    store.Maintenance.Send(new CreateSampleDataOperation(DatabaseItemType.Indexes));

                    // the database loads after all indexes are loaded
                    var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                    var lines = File.ReadAllLines(outputFile);
                    Assert.Equal(10, lines.Length);
                    Assert.True(lines[0].Contains($"{DirectoryExecUtils.EnvironmentType.System} {SystemDbName} {options.BasePath} {options.TempPath} {options.JournalPath}"));
                    Assert.True(lines[1].Contains($"{DirectoryExecUtils.EnvironmentType.Database} {store.Database} {options.BasePath} {options.TempPath} {options.JournalPath}"));
                    Assert.True(lines[2].Contains($"{DirectoryExecUtils.EnvironmentType.Configuration} {store.Database} {options.BasePath} {options.TempPath} {options.JournalPath}"));

                    var indexes = documentDatabase.IndexStore.GetIndexes().ToArray();

                    Assert.Equal(7, indexes.Length);

                    // The indexes order in the IndexStore don't match the order of storage env creation and we need a one-to-one match.
                    var matches = lines.ToList().GetRange(3, 7);

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

        [Fact]
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
                store.Maintenance.Send(new CreateSampleDataOperation(DatabaseItemType.Indexes));

                // The database loads after all indexes are loaded
                var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                var lines = File.ReadAllLines(outputFile);
                Assert.Equal(10, lines.Length);

                var systemEnvOptions = Server.ServerStore._env.Options;
                var configEnvOptions = documentDatabase.ConfigurationStorage.Environment.Options;
                var docsEnvOptions = documentDatabase.DocumentsStorage.Environment.Options;

                Assert.True(lines[0].Contains($"{DirectoryExecUtils.EnvironmentType.System} {SystemDbName} {systemEnvOptions.BasePath} {systemEnvOptions.TempPath} {systemEnvOptions.JournalPath}"));
                Assert.True(lines[1].Contains($"{DirectoryExecUtils.EnvironmentType.Database} {store.Database} {docsEnvOptions.BasePath} {docsEnvOptions.TempPath} {docsEnvOptions.JournalPath}"));
                Assert.True(lines[2].Contains($"{DirectoryExecUtils.EnvironmentType.Configuration} {store.Database} {configEnvOptions.BasePath} {configEnvOptions.TempPath} {configEnvOptions.JournalPath}"));

                var indexes = documentDatabase.IndexStore.GetIndexes().ToArray();

                Assert.Equal(7, indexes.Length);

                // The indexes order in the IndexStore don't match the order of storage env creation and we need a one-to-one match.
                var matches = lines.ToList().GetRange(3, 7);

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

        [Fact]
        public void CanGetErrorsFromOnDirectoryInitialize()
        {
            string script;
            IDictionary<string, string> customSettings = new ConcurrentDictionary<string, string>();
            var scriptFile = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            var outputFile = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".txt"));
            if (PlatformDetails.RunningOnPosix)
            {
                customSettings[RavenConfiguration.GetKey(x => x.Storage.OnDirectoryInitializeExec)] = "bash";
                customSettings[RavenConfiguration.GetKey(x => x.Storage.OnDirectoryInitializeExecArguments)] = $"{scriptFile} {outputFile}";
                script = "#!/bin/bash\necho \"ERROR!\nKarmelush is ANGRY\" >&2\nexit 129";
                File.WriteAllText(scriptFile, script);
                Process.Start("chmod", $"700 {scriptFile}");
            }
            else
            {
                customSettings[RavenConfiguration.GetKey(x => x.Storage.OnDirectoryInitializeExec)] = "powershell";
                customSettings[RavenConfiguration.GetKey(x => x.Storage.OnDirectoryInitializeExecArguments)] = $"{scriptFile} {outputFile}";
                script = @"$stderr = [System.Console]::OpenStandardError()
$errorLine1 = [system.Text.Encoding]::UTF8.GetBytes(""ERROR!\r\n"")
$errorLine2 = [system.Text.Encoding]::UTF8.GetBytes(""Karmelush is ANGRY\r\n"")
$stderr.Write($errorLine1, 0, $errorLine1.Length)
$stderr.Write($errorLine2, 0, $errorLine2.Length)
exit 129";
                File.WriteAllText(scriptFile, script);
            }
            var e = Assert.ThrowsAny<Exception>(() =>
            {
                UseNewLocalServer(customSettings: customSettings, runInMemory: false);
            });
            Assert.True(e.InnerException.Message.Contains("ERROR!") && e.InnerException.Message.Contains("Karmelush is ANGRY"));
        }

        [Fact]
        public void CanGetCpuCreditsFromExec()
        {
            string script;
            IDictionary<string, string> customSettings = new ConcurrentDictionary<string, string>();
            var scriptFile = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));

            var timestamp = $"{DateTime.UtcNow:s}";

            var jsonCpuCredits = "{\"Remaining\":34.665476, \"Timestamp\":\"" + timestamp + "\"}";

            if (PlatformDetails.RunningOnPosix)
            {
                customSettings[RavenConfiguration.GetKey(x => x.Server.CpuCreditsExec)] = "bash";
                customSettings[RavenConfiguration.GetKey(x => x.Server.CpuCreditsExecArguments)] = $"{scriptFile}";
                customSettings[RavenConfiguration.GetKey(x => x.Server.CpuCreditsExhaustionBackgroundTasksThreshold)] = "15";
                customSettings[RavenConfiguration.GetKey(x => x.Server.CpuCreditsExhaustionFailoverThreshold)] = "30";
                customSettings[RavenConfiguration.GetKey(x => x.Server.CpuCreditsMax)] = "4320";
                customSettings[RavenConfiguration.GetKey(x => x.Server.CpuCreditsBase)] = "180";

                script = "#!/bin/bash\necho " + jsonCpuCredits.Replace("\"", "\\\"");

                File.WriteAllText(scriptFile, script);
                Process.Start("chmod", $"700 {scriptFile}");
            }
            else
            {
                customSettings[RavenConfiguration.GetKey(x => x.Server.CpuCreditsExec)] = "powershell";
                customSettings[RavenConfiguration.GetKey(x => x.Server.CpuCreditsExecArguments)] = $"-NoProfile {scriptFile}";
                customSettings[RavenConfiguration.GetKey(x => x.Server.CpuCreditsExhaustionBackgroundTasksThreshold)] = "15";
                customSettings[RavenConfiguration.GetKey(x => x.Server.CpuCreditsExhaustionFailoverThreshold)] = "30";
                customSettings[RavenConfiguration.GetKey(x => x.Server.CpuCreditsMax)] = "4320";
                customSettings[RavenConfiguration.GetKey(x => x.Server.CpuCreditsBase)] = "180";

                jsonCpuCredits = $"\"{jsonCpuCredits.Replace("\"", "`\"")}\"";

                script = $@"Write-Host {jsonCpuCredits}";

                File.WriteAllText(scriptFile, script);
            }

            UseNewLocalServer(customSettings: customSettings, runInMemory: false);

            var value = Server.UpdateCpuCreditsFromExec();

            Assert.Equal(34.665476, value);
        }

        [Fact]
        public async Task CertificateAndMasterKeyExecTest()
        {
            string script;
            IDictionary<string, string> customSettings = new ConcurrentDictionary<string, string>();
            var keyPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var buffer = new byte[256 / 8];
            using (var cryptoRandom = RandomNumberGenerator.Create())
            {
                cryptoRandom.GetBytes(buffer);
            }
            File.WriteAllBytes(keyPath, buffer);
            var certificates = Certificates.GenerateAndSaveSelfSignedCertificate();
            if (PlatformDetails.RunningOnPosix)
            {
                var scriptPath = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".sh"));
                var keyArgs = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { scriptPath, keyPath });
                var certArgs = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { scriptPath, certificates.ServerCertificatePath });

                customSettings[RavenConfiguration.GetKey(x => x.Security.MasterKeyExec)] = "bash";
                customSettings[RavenConfiguration.GetKey(x => x.Security.MasterKeyExecArguments)] = $"{keyArgs}";
                customSettings[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec)] = "bash";
                customSettings[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExecArguments)] = $"{certArgs}";
                customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = "https://" + Environment.MachineName + ":0";

                script = "#!/bin/bash\ncat \"$1\"";
                File.WriteAllText(scriptPath, script);
                Process.Start("chmod", $"700 {scriptPath}");
            }
            else
            {
                var scriptPath = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
                var keyArgs = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { "-NoProfile", scriptPath, keyPath });
                var certArgs = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { "-NoProfile", scriptPath, certificates.ServerCertificatePath });

                customSettings[RavenConfiguration.GetKey(x => x.Security.MasterKeyExec)] = "powershell";
                customSettings[RavenConfiguration.GetKey(x => x.Security.MasterKeyExecArguments)] = $"{keyArgs}";
                customSettings[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec)] = "powershell";
                customSettings[RavenConfiguration.GetKey(x => x.Security.CertificateRenewExec)] = "powershell";
                customSettings[RavenConfiguration.GetKey(x => x.Security.CertificateChangeExec)] = "powershell";
                customSettings[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExecArguments)] = $"{certArgs}";
                customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = "https://" + Environment.MachineName + ":0";
                script = @"param([string]$userArg)
try {
    $bytes = Get-Content -path $userArg -encoding Byte
    $stdout = [System.Console]::OpenStandardOutput()
    $stdout.Write($bytes, 0, $bytes.Length)
}
catch {
    Write-Error $_.Exception
    exit 1
}
exit 0";
                File.WriteAllText(scriptPath, script);
            }

            UseNewLocalServer(customSettings: customSettings, runInMemory: false);
            // The master key loading is lazy, let's put a database secret key to invoke it.
            var dbName = GetDatabaseName();
            var databaseKey = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(databaseKey);
            }
            var base64Key = Convert.ToBase64String(databaseKey);
            // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
            try
            {
#pragma warning disable CA1416 // Validate platform compatibility
                ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
#pragma warning restore CA1416 // Validate platform compatibility
            }
            catch (PlatformNotSupportedException)
            {
                return;
            }

            await Server.ServerStore.EnsureNotPassiveAsync();
            Server.ServerStore.PutSecretKey(base64Key, dbName, true);
            X509Certificate2 serverCertificate;
            try
            {
                serverCertificate = CertificateHelper.CreateCertificate(certificates.ServerCertificatePath, (string)null, CertificateLoaderUtil.FlagsForExport | X509KeyStorageFlags.MachineKeySet);
            }
            catch (CryptographicException e)
            {
                throw new CryptographicException($"Failed to load the test certificate from {certificates}.", e);
            }
            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = serverCertificate,
                ClientCertificate = serverCertificate,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                Path = NewDataPath()
            }))
            {
            }
            var secrets = Server.ServerStore.Secrets;
            var serverMasterKey = (Lazy<byte[]>)typeof(SecretProtection).GetField("_serverMasterKey", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(secrets);
            Assert.True(serverMasterKey.Value.SequenceEqual(buffer));
            Assert.True(Server.Certificate.Certificate.Equals(serverCertificate));
        }

        [RavenFact(RavenTestCategory.Certificates)]
        public async Task RenewCertificate_WhenGetTheSame_ShouldNotTriggerUpdatedServerCertificate()
        {
            var customSettings = new ConcurrentDictionary<string, string>();
            var certificates = Certificates.GenerateAndSaveSelfSignedCertificate();

            (string exe, string certArgs) certProcess;
            if (PlatformDetails.RunningOnPosix)
            {
                var scriptPath = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".sh"));
                certProcess.certArgs = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { scriptPath, certificates.ServerCertificatePath });
                certProcess.exe = "bash";

                var script = "#!/bin/bash\ncat \"$1\"";
                await File.WriteAllTextAsync(scriptPath, script);
                Process.Start("chmod", $"700 {scriptPath}");
            }
            else
            {
                var scriptPath = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
                certProcess.certArgs = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { "-NoProfile", scriptPath, certificates.ServerCertificatePath });

                certProcess.exe = "powershell";
                var script = @"param([string]$userArg)
try {
    $bytes = Get-Content -path $userArg -encoding Byte
    $stdout = [System.Console]::OpenStandardOutput()
    $stdout.Write($bytes, 0, $bytes.Length)
}
catch {
    Write-Error $_.Exception
    exit 1
}
exit 0";
                await File.WriteAllTextAsync(scriptPath, script);
            }

            customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = "https://" + Environment.MachineName + ":0";
            customSettings[RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = certificates.ServerCertificatePath;
            customSettings[RavenConfiguration.GetKey(x => x.Security.CertificateRenewExec)] = certProcess.exe;
            customSettings[RavenConfiguration.GetKey(x => x.Security.CertificateRenewExecArguments)] = certProcess.certArgs;

            UseNewLocalServer(customSettings: customSettings, runInMemory: false);
            // The master key loading is lazy, let's put a database secret key to invoke it.
            X509Certificate2 serverCertificate;
            try
            {
                serverCertificate = CertificateHelper.CreateCertificate(certificates.ServerCertificatePath, (string)null,
                    CertificateLoaderUtil.FlagsForExport | X509KeyStorageFlags.MachineKeySet);
            }
            catch (CryptographicException e)
            {
                throw new CryptographicException($"Failed to load the test certificate from {certificates}.", e);
            }

            var ts = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Server.ServerStore.Engine.StateMachine.Changes.ValueChanged += (index, type) =>
            {
                if (type == nameof(InstallUpdatedServerCertificateCommand))
                {
                    ts.TrySetResult();
                }

                return Task.CompletedTask;
            };
            
            using (var store = GetDocumentStore(new Options
                   {
                       AdminCertificate = serverCertificate,
                       ClientCertificate = serverCertificate,
                   }))
            {
                var httpClient = store.GetRequestExecutor().HttpClient;
                var response = await httpClient.SendAsync(new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    RequestUri = new Uri($"{Server.WebUrl}/admin/certificates/refresh")
                });
                Assert.True(response.IsSuccessStatusCode);

                Assert.NotEqual(ts.Task, await Task.WhenAny(ts.Task, Task.Delay(5 * 1000)));
            }
        }
    }
}
