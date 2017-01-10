// -----------------------------------------------------------------------
//  <copyright file="RavenFsTestBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util.Encryptors;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.FileSystem;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.FileSystem;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Server;
using Raven.Tests.Helpers.Util;

namespace Raven.Tests.Helpers
{
    public class RavenFilesTestBase : IDisposable
    {
        private static int pathCount;

        public static IEnumerable<object[]> Storages
        {
            get
            {
                return new[]
                {
                    new object[] {"voron"},
                    new object[] {"esent"}
                };
            }
        }

        private readonly List<RavenDbServer> servers = new List<RavenDbServer>();
        private readonly List<IFilesStore> filesStores = new List<IFilesStore>();
        private readonly List<IAsyncFilesCommands> asyncCommandClients = new List<IAsyncFilesCommands>();
        private readonly HashSet<string> pathsToDelete = new HashSet<string>();
        protected static readonly int[] Ports = { 8079, 8078, 8077, 8076, 8075 };

        protected TimeSpan SynchronizationInterval { get; set; }

        private static bool checkedAsyncVoid;
        protected RavenFilesTestBase()
        {
            if (checkedAsyncVoid == false)
            {
                checkedAsyncVoid = true;
                RavenTestBase.AssertNoAsyncVoidMethods(GetType().Assembly);
            }
            this.SynchronizationInterval = TimeSpan.FromMinutes(10);
        }

        protected RavenDbServer CreateServer(int port,
                                                    string dataDirectory = null,
                                                    bool runInMemory = true,
                                                    string requestedStorage = null,
                                                    bool enableAuthentication = false,
                                                    string fileSystemName = null,
                                                    string activeBundles = null,
                                                    Action<RavenConfiguration> customConfig = null)
        {
            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);
            var storageType = GetDefaultStorageType(requestedStorage);
            var directory = dataDirectory ?? NewDataPath(fileSystemName + "_" + port);

            var ravenConfiguration = new RavenConfiguration();

            if (customConfig != null)
            {
                customConfig(ravenConfiguration);
            }

            ravenConfiguration.Port = port;
            ravenConfiguration.DataDirectory = directory;
            ravenConfiguration.RunInMemory = runInMemory;
#if DEBUG
            ravenConfiguration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction = runInMemory;
#endif
            ravenConfiguration.DefaultStorageTypeName = storageType;
            ravenConfiguration.AnonymousUserAccessMode = enableAuthentication ? AnonymousUserAccessMode.None : AnonymousUserAccessMode.Admin;
            ravenConfiguration.Encryption.UseFips = ConfigurationHelper.UseFipsEncryptionAlgorithms;
            ravenConfiguration.MaxSecondsForTaskToWaitForDatabaseToLoad = 20;

            ravenConfiguration.FileSystem.MaximumSynchronizationInterval = SynchronizationInterval;
            ravenConfiguration.FileSystem.DataDirectory = Path.Combine(directory, "FileSystems");
            ravenConfiguration.FileSystem.DefaultStorageTypeName = storageType;

            if (activeBundles != null)
            {
                ravenConfiguration.Settings[Constants.ActiveBundles] = activeBundles;
            }

            if (enableAuthentication)
            {
                Authentication.EnableOnce();
            }

            var ravenDbServer = new RavenDbServer(ravenConfiguration)
            {
                UseEmbeddedHttpServer = true,
            };

            ravenDbServer.Initialize();

            servers.Add(ravenDbServer);

            if (enableAuthentication)
            {
                EnableAuthentication(ravenDbServer.SystemDatabase);
            }

            ConfigureServer(ravenDbServer, fileSystemName);

            return ravenDbServer;
        }

        protected virtual FilesStore NewStore(int index = 0, bool fiddler = false, bool enableAuthentication = false, string apiKey = null,
                                                ICredentials credentials = null, string requestedStorage = null, [CallerMemberName] string fileSystemName = null,
                                                bool runInMemory = true, Action<RavenConfiguration> customConfig = null, string activeBundles = null, string connectionStringName = null)
        {
            fileSystemName = NormalizeFileSystemName(fileSystemName);

            var server = CreateServer(Ports[index],
                fileSystemName: fileSystemName,
                enableAuthentication: enableAuthentication,
                customConfig: customConfig,
                requestedStorage: requestedStorage,
                runInMemory: runInMemory,
                activeBundles: activeBundles);

            server.Url = GetServerUrl(fiddler, server.SystemDatabase.ServerUrl);

            var store = new FilesStore
            {
                Url = server.Url,
                DefaultFileSystem = fileSystemName,
                Credentials = credentials,
                ApiKey = apiKey,
                ConnectionStringName = connectionStringName
            };

            store.AfterDispose += (sender, args) => server.Dispose();

            ModifyStore(store);

            store.Initialize(ensureFileSystemExists: true);

            this.filesStores.Add(store);

            return store;
        }

        protected virtual IAsyncFilesCommands NewAsyncClient(int index = 0,
            bool fiddler = false,
            bool enableAuthentication = false,
            string apiKey = null,
            ICredentials credentials = null,
            string requestedStorage = null,
            [CallerMemberName] string fileSystemName = null,
            Action<RavenConfiguration> customConfig = null,
            string activeBundles = null,
            string dataDirectory = null,
            bool runInMemory = true)
        {
            fileSystemName = NormalizeFileSystemName(fileSystemName);

            var server = CreateServer(Ports[index], fileSystemName: fileSystemName, enableAuthentication: enableAuthentication, requestedStorage: requestedStorage, activeBundles: activeBundles, customConfig: customConfig,
                dataDirectory: dataDirectory, runInMemory: runInMemory);
            server.Url = GetServerUrl(fiddler, server.SystemDatabase.ServerUrl);

            var store = new FilesStore
            {
                Url = server.Url,
                DefaultFileSystem = fileSystemName,
                Credentials = credentials,
                ApiKey = apiKey,
            };

            store.AfterDispose += (sender, args) => server.Dispose();

            ModifyStore(store);
            store.Initialize(true);

            filesStores.Add(store);

            var client = store.AsyncFilesCommands;
            asyncCommandClients.Add(client);

            return client;
        }

        protected RavenFileSystem GetFileSystem(int index = 0, [CallerMemberName] string fileSystemName = null)
        {
            fileSystemName = NormalizeFileSystemName(fileSystemName);

            return servers.First(x => x.SystemDatabase.Configuration.Port == Ports[index]).Server.GetRavenFileSystemInternal(fileSystemName).Result;
        }

        protected RavenDbServer GetServer(int index = 0)
        {
            return servers.First(x => x.SystemDatabase.Configuration.Port == Ports[index]);
        }

        protected static string GetServerUrl(bool fiddler, string serverUrl)
        {
            if (fiddler)
            {
                if (Process.GetProcessesByName("fiddler").Any())
                    return serverUrl.Replace("localhost", "localhost.fiddler");
            }

            return serverUrl;
        }

        protected virtual void ConfigureServer(RavenDbServer server, string fileSystemName)
        {
        }

        protected string NewDataPath(string prefix = null, bool deleteOnDispose = true)
        {
            // Federico: With a filesystem name too large, we can easily pass the filesystem path limit. 
            // The truncation of the Guid to 8 still provides enough entropy to avoid collisions.
            string suffix = Guid.NewGuid().ToString("N").Substring(0, 8);

            var newDataDir = Path.GetFullPath(string.Format(@".\{0}-{1}-{2}-{3}\", DateTime.Now.ToString("yyyy-MM-dd,HH-mm-ss"), prefix ?? "RavenFS_Test", suffix, Interlocked.Increment(ref pathCount)));
            Directory.CreateDirectory(newDataDir);

            if (deleteOnDispose)
                pathsToDelete.Add(newDataDir);
            return newDataDir;
        }

        protected string NormalizeFileSystemName(string fileSystemName)
        {
            if (string.IsNullOrEmpty(fileSystemName))
                return null;

            if (fileSystemName.Length < 50)
                return fileSystemName;

            // Federico: With a too large value (say 30) it is very easy to pass the filesystem limit when the test name is large. 
            var prefix = fileSystemName.Substring(0, 10);
            var suffix = fileSystemName.Substring(fileSystemName.Length - 5, 5);
            var hash = new Guid(Encryptor.Current.Hash.Compute16(Encoding.UTF8.GetBytes(fileSystemName))).ToString("N").Substring(0, 8);

            return string.Format("{0}_{1}_{2}", prefix, hash, suffix);
        }

        public static string GetDefaultStorageType(string requestedStorage = null)
        {
            string defaultStorageType;
            var envVar = Environment.GetEnvironmentVariable("raventest_storage_engine");
            if (string.IsNullOrEmpty(envVar) == false)
                defaultStorageType = envVar;
            else if (requestedStorage != null)
                defaultStorageType = requestedStorage;
            else
                defaultStorageType = "voron";
            return defaultStorageType;
        }

        protected static string StreamToString(Stream stream)
        {
            using (var reader = new StreamReader(stream, Encoding.UTF8))
            {
                return reader.ReadToEnd();
            }
        }

        protected static Stream StringToStream(string src)
        {
            byte[] byteArray = Encoding.UTF8.GetBytes(src);
            return new MemoryStream(byteArray);
        }

        public static void EnableAuthentication(DocumentDatabase database)
        {
            var license = GetLicenseByReflection(database);
            license.Error = false;
            license.Status = "Commercial";

            // rerun this startup task
            database.StartupTasks.OfType<AuthenticationForCommercialUseOnly>().First().Execute(database);
        }

        private static LicensingStatus GetLicenseByReflection(DocumentDatabase database)
        {
            var field = database.GetType().GetField("initializer", BindingFlags.Instance | BindingFlags.NonPublic);
            var initializer = field.GetValue(database);
            var validateLicenseField = initializer.GetType().GetField("validateLicense", BindingFlags.Instance | BindingFlags.NonPublic);
            var validateLicense = validateLicenseField.GetValue(initializer);

            var currentLicenseProp = validateLicense.GetType().GetProperty("CurrentLicense", BindingFlags.Static | BindingFlags.Public);

            return (LicensingStatus)currentLicenseProp.GetValue(validateLicense, null);
        }

        public virtual void Dispose()
        {
            var errors = new List<Exception>();

            foreach (var client in asyncCommandClients)
            {
                try
                {
                    client.Dispose();
                }
                catch (Exception e)
                {
                    errors.Add(e);
                }
            }
            asyncCommandClients.Clear();

            foreach (var fileStore in filesStores)
            {
                try
                {
                    fileStore.Dispose();
                }
                catch (Exception e)
                {
                    errors.Add(e);
                }
            }
            filesStores.Clear();

            foreach (var server in servers)
            {
                try
                {
                    server.Dispose();
                }
                catch (Exception e)
                {
                    errors.Add(e);
                }
            }

            servers.Clear();

            GC.Collect(2);
            GC.WaitForPendingFinalizers();

            foreach (var pathToDelete in pathsToDelete)
            {
                try
                {
                    ClearDatabaseDirectory(pathToDelete);
                }
                catch (Exception e)
                {
                    errors.Add(e);
                }
                finally
                {
                    if (Directory.Exists(pathToDelete) ||
                        File.Exists(pathToDelete)	// Just in order to be sure we didn't created a file in that path, by mistake
                        )
                    {
                        errors.Add(new IOException(string.Format("We tried to delete the '{0}' directory, but failed", pathToDelete)));
                    }
                }
            }

            if (errors.Count > 0)
                throw new AggregateException(errors);
        }

        protected void ClearDatabaseDirectory(string dataDir)
        {
            bool isRetry = false;

            while (true)
            {
                try
                {
                    IOExtensions.DeleteDirectory(dataDir);
                    break;
                }
                catch (IOException)
                {
                    if (isRetry)
                        throw;

                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    isRetry = true;

                    Thread.Sleep(2500);
                }
            }
        }

        protected Stream CreateUniformFileStream(int size, char value = 'a')
        {
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string(value, size);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;

            return ms;
        }

        private Random generator = new Random();

        protected void ReseedRandom(int seed)
        {
            generator = new Random(seed);
        }

        protected Stream CreateRandomFileStream(int size)
        {
            var ms = new MemoryStream();

            // Write in blocks
            byte[] buffer = new byte[4096];
            for (int i = 0; i < size / buffer.Length; i++)
            {
                generator.NextBytes(buffer);
                ms.Write(buffer, 0, buffer.Length);
            }

            // Write last block
            buffer = new byte[size % buffer.Length];
            if (buffer.Length != 0)
            {
                generator.NextBytes(buffer);
                ms.Write(buffer, 0, buffer.Length);
            }

            ms.Flush();
            ms.Position = 0;

            return ms;
        }

        protected void WaitForFile(IAsyncFilesCommands filesCommands, string fileName)
        {
            var done = SpinWait.SpinUntil(() =>
            {
                var file = filesCommands.GetMetadataForAsync(fileName).Result;
                return file != null;
            }, TimeSpan.FromSeconds(15));

            if (!done) throw new Exception("WaitForDocument failed");
        }

        protected async Task WaitForOperationAsync(string url, long operationId)
        {
            using (var sysDbStore = new DocumentStore
            {
                Url = url
            }.Initialize())
            {
                await new Operation((AsyncServerClient)sysDbStore.AsyncDatabaseCommands, operationId).WaitForCompletionAsync();
            }
        }

        protected virtual void ModifyStore(FilesStore store)
        {
        }
    }
}
