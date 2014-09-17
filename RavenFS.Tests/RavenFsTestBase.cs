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
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util.Encryptors;
using Raven.Client.FileSystem.Extensions;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.RavenFS;
using Raven.Database.Server.Security;
using Raven.Server;
using Raven.Client.FileSystem;

using RavenFS.Tests.Tools;

using Xunit;

namespace RavenFS.Tests
{
    public class RavenFsTestBase : WithNLog, IDisposable
    {
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
        public static readonly int[] Ports = { 19067, 19068, 19069 };

        public TimeSpan SynchronizationInterval { get; protected set; }

        protected RavenFsTestBase()
        {
            this.SynchronizationInterval = TimeSpan.FromMinutes(10);
        }

        protected RavenDbServer CreateRavenDbServer(int port,
                                                    string dataDirectory = null,
                                                    bool runInMemory = true,
                                                    string requestedStorage = null,
                                                    bool enableAuthentication = false,
                                                    string fileSystemName = null,
                                                    Action<RavenConfiguration> customConfig = null)
        {
            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);
            var storageType = GetDefaultStorageType(requestedStorage);
            var directory = dataDirectory ?? NewDataPath(fileSystemName + "_" + port);

            var ravenConfiguration = new RavenConfiguration()
            {
				Port = port,
				DataDirectory = directory,
				RunInMemory = storageType.Equals("esent", StringComparison.OrdinalIgnoreCase) == false && runInMemory,
#if DEBUG
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = runInMemory,
#endif
				DefaultStorageTypeName = storageType,
				AnonymousUserAccessMode = enableAuthentication ? AnonymousUserAccessMode.None : AnonymousUserAccessMode.Admin,                
			};

            if (customConfig != null)
            {
                customConfig(ravenConfiguration);
                ravenConfiguration.Initialize();
            }

	        ravenConfiguration.Encryption.UseFips = SettingsHelper.UseFipsEncryptionAlgorithms;
            ravenConfiguration.FileSystem.MaximumSynchronizationInterval = this.SynchronizationInterval;
	        ravenConfiguration.FileSystem.DataDirectory = Path.Combine(directory, "FileSystem");
	        ravenConfiguration.FileSystem.DefaultStorageTypeName = storageType;

            if (enableAuthentication)
            {
                Authentication.EnableOnce();
            }

            var ravenDbServer = new RavenDbServer(ravenConfiguration)
            {
	            UseEmbeddedHttpServer = true
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

        protected virtual IFilesStore NewStore( int index = 0, bool fiddler = false, bool enableAuthentication = false, string apiKey = null, 
                                                ICredentials credentials = null, string requestedStorage = null, [CallerMemberName] string fileSystemName = null, 
                                                bool runInMemory = true, Action<RavenConfiguration> customConfig = null)
        {
            fileSystemName = NormalizeFileSystemName(fileSystemName);

            var server = CreateRavenDbServer(Ports[index], 
                fileSystemName: fileSystemName,
                enableAuthentication: enableAuthentication, 
                customConfig: customConfig,
                requestedStorage: requestedStorage, 
                runInMemory:runInMemory);

            var store = new FilesStore()
            {
                Url = GetServerUrl(fiddler, server.SystemDatabase.ServerUrl),
                DefaultFileSystem = fileSystemName,
                Credentials = credentials,
                ApiKey = apiKey,                 
            };

            store.Initialize(true);

            this.filesStores.Add(store);

            return store;                        
        }

        protected virtual IAsyncFilesCommands NewAsyncClient(int index = 0, bool fiddler = false, bool enableAuthentication = false, string apiKey = null, 
                                                             ICredentials credentials = null, string requestedStorage = null, [CallerMemberName] string fileSystemName = null)
        {
            fileSystemName = NormalizeFileSystemName(fileSystemName);

            var server = CreateRavenDbServer(Ports[index], fileSystemName: fileSystemName, enableAuthentication: enableAuthentication, requestedStorage: requestedStorage);

            var store = new FilesStore()
            {
                Url = GetServerUrl(fiddler, server.SystemDatabase.ServerUrl),
                DefaultFileSystem = fileSystemName,
                Credentials = credentials,
                ApiKey = apiKey,
            };

            store.Initialize(true);

            this.filesStores.Add(store);

            var client = store.AsyncFilesCommands;
            asyncCommandClients.Add(client);

            return client;       
        }

        protected RavenFileSystem GetRavenFileSystem(int index = 0, [CallerMemberName] string fileSystemName = null)
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

        protected string NewDataPath(string prefix = null)
        {
            // Federico: With a filesystem name too large, we can easily pass the filesystem path limit. 
            // The truncation of the Guid to 8 still provides enough entropy to avoid collisions.
            string suffix = Guid.NewGuid().ToString("N").Substring(0, 8);

            var newDataDir = Path.GetFullPath(string.Format(@".\{0}-{1}-{2}\", DateTime.Now.ToString("yyyy-MM-dd,HH-mm-ss"), prefix ?? "RavenFS_Test", suffix));
            Directory.CreateDirectory(newDataDir);
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

        public static string StreamToString(Stream stream)
        {
            using (var reader = new StreamReader(stream, Encoding.UTF8))
            {
                return reader.ReadToEnd();
            }
        }

        public static void EnableAuthentication(DocumentDatabase database)
        {
            var license = GetLicenseByReflection(database);
            license.Error = false;
            license.Status = "Commercial";

            // rerun this startup task
            database.StartupTasks.OfType<AuthenticationForCommercialUseOnly>().First().Execute(database);
        }

        public static LicensingStatus GetLicenseByReflection(DocumentDatabase database)
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

        protected void WaitForBackup(string fileStoreName, bool checkError)
        {
            Func<string, JsonDocument> getDocument = fsName => servers.First().SystemDatabase.Documents.Get(BackupStatus.RavenFilesystemBackupStatusDocumentKey(fsName), null);
            var done = SpinWait.SpinUntil(() =>
            {
                // We expect to get the doc from database that we tried to backup
                var jsonDocument = getDocument(fileStoreName);
                if (jsonDocument == null)
                    return true;

                var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
                if (backupStatus.IsRunning == false)
                {
                    if (checkError)
                    {
                        var firstOrDefault =
                            backupStatus.Messages.FirstOrDefault(x => x.Severity == BackupStatus.BackupMessageSeverity.Error);
                        if (firstOrDefault != null)
                            Assert.True(false, string.Format("{0}\n\nDetails: {1}", firstOrDefault.Message, firstOrDefault.Details));
                    }

                    return true;
                }
                return false;
            }, Debugger.IsAttached ? TimeSpan.FromMinutes(120) : TimeSpan.FromMinutes(15));
            Assert.True(done);
        }
    }
}