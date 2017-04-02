#if DNXCORE50
using Raven.Abstractions.Connection;
using Raven.Client;
using Raven.Client.Document;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;

namespace Raven.Tests.Core
{
    public class TestServerFixture : IDisposable
    {
        public const int Port = 8070;
        public const string ServerName = "Raven.Tests.Core.Server";

        public IDocumentStore DocumentStore { get; private set; }

        public static string ServerUrl { get; private set; }

        private Process process;

        public TestServerFixture()
        {
            StartProcess();

            CreateServerAsync().Wait();
            CreateStore();
        }

        private void CreateStore()
        {
            var store = new DocumentStore { Url = ServerUrl };
            store.Initialize();

            DocumentStore = store;
        }

        private static async Task CreateServerAsync()
        {
            var httpClient = new HttpClient();
            var serverConfiguration = new ServerConfiguration
            {
                DefaultStorageTypeName = "voron",
                Port = Port,
                RunInMemory = true,
                Settings =
                {
                    { "Raven/ServerName", ServerName }
                }
            };

            var response = await httpClient
                .PutAsync("http://localhost:8585/servers", new JsonContent(RavenJObject.FromObject(serverConfiguration)))
                .ConfigureAwait(false);

            if (response.IsSuccessStatusCode == false)
                throw new InvalidOperationException("Failed to start server.");

            using (var stream = await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false))
            {
                var data = RavenJToken.TryLoad(stream);
                if (data == null)
                    throw new InvalidOperationException("Failed to retrieve server url.");

                ServerUrl = data.Value<string>("ServerUrl");
            }
        }

        private void StartProcess()
        {
            KillServerRunner();

            var path = GetServerRunnerPath();
            var startInfo = new ProcessStartInfo(path)
            {
                CreateNoWindow = true,
                UseShellExecute = false
            };

            process = Process.Start(startInfo);
        }

        public void Dispose()
        {
            if (DocumentStore != null)
                DocumentStore.Dispose();

            try
            {
                process?.Kill();
            }
            catch (Exception)
            {
            }

            KillServerRunner();
        }

        private static void KillServerRunner()
        {
            var processes = Process.GetProcessesByName("Raven.Tests.Server.Runner.exe");
            foreach (var p in processes)
            {
                try
                {
                    p.Kill();
                }
                catch (Exception)
                {
                }
            }
        }

        private static string GetServerRunnerPath()
        {
#if DEBUG
            var path = "Raven.Tests.Server.Runner/bin/Debug/Raven.Tests.Server.Runner.exe";
#else
            var path = "Raven.Tests.Server.Runner/bin/Release/Raven.Tests.Server.Runner.exe";
#endif

            var tries = 10;
            while (tries > 0)
            {
                path = Path.Combine("../", path);
                var fullPath = Path.GetFullPath(path);

                if (File.Exists(fullPath))
                {
                    path = fullPath;
                    break;
                }

                tries--;
            }

            if (File.Exists(path) == false)
                throw new InvalidOperationException(string.Format("Could not locate 'Raven.Tests.Server.Runner' in '{0}'.", path));

            return path;
        }

        private class ServerConfiguration
        {
            public ServerConfiguration()
            {
                Settings = new Dictionary<string, string>();
            }

            public int Port { get; set; }

            public bool RunInMemory { get; set; }

            public string DefaultStorageTypeName { get; set; }

            public bool UseCommercialLicense { get; set; }

            public string ApiKeyName { get; set; }

            public string ApiKeySecret { get; set; }

            public IDictionary<string, string> Settings { get; set; }

            public bool HasApiKey { get { return !string.IsNullOrEmpty(ApiKeyName) && !string.IsNullOrEmpty(ApiKeySecret); } }
        }
    }
}
#endif