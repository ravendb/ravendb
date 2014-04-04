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
using Raven.Abstractions.Util.Encryptors;
using Raven.Client.RavenFS;
using Raven.Client.RavenFS.Extensions;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.RavenFS;
using Raven.Database.Server.Security;
using Raven.Server;
using Xunit;

namespace RavenFS.Tests
{
    public class RavenFsTestBase : WithNLog, IDisposable
    {
        private readonly List<RavenDbServer> servers = new List<RavenDbServer>();
        private readonly List<RavenFileSystemClient> ravenFileSystemClients = new List<RavenFileSystemClient>();
        private readonly HashSet<string> pathsToDelete = new HashSet<string>();
        public static readonly int[] Ports = { 19079, 19078, 19077 };

        protected RavenDbServer CreateRavenDbServer(int port,
                                                    string dataDirectory = null,
                                                    bool runInMemory = true,
                                                    string requestedStorage = null,
                                                    bool enableAuthentication = false,
                                                    string fileSystemName = null)
        {
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

        protected virtual RavenFileSystemClient NewClient(int index = 0, bool fiddler = false, bool enableAuthentication = false, string apiKey = null, 
                                                          ICredentials credentials = null, string requestedStorage = null, [CallerMemberName] string fileSystemName = null)
        {
            fileSystemName = NormalizeFileSystemName(fileSystemName);

            var server = CreateRavenDbServer(Ports[index], fileSystemName: fileSystemName, enableAuthentication: enableAuthentication, requestedStorage: requestedStorage);

            var client = new RavenFileSystemClient(GetServerUrl(fiddler, server.SystemDatabase.ServerUrl), fileSystemName, apiKey: apiKey, credentials: credentials);

            client.EnsureFileSystemExistsAsync().Wait();

            ravenFileSystemClients.Add(client);

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
            var newDataDir = Path.GetFullPath(string.Format(@".\{0}-{1}-{2}\", DateTime.Now.ToString("yyyy-MM-dd,HH-mm-ss"), prefix ?? "RavenFS_Test", Guid.NewGuid().ToString("N")));
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

            var prefix = fileSystemName.Substring(0, 30);
            var suffix = fileSystemName.Substring(fileSystemName.Length - 10, 10);
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
            stream.Position = 0;
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

            foreach (var client in ravenFileSystemClients)
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

            ravenFileSystemClients.Clear();

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
    }
}