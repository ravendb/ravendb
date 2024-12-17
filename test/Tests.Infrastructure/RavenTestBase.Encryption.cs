using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server;
using Sparrow.Platform;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly EncryptionTestBase Encryption;

    public class EncryptionTestBase
    {
        private readonly Dictionary<(RavenServer Server, string Database), string> _serverDatabaseToMasterKey = new();

        private readonly RavenTestBase _parent;

        public EncryptionTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public async Task<EncryptServerResult> EncryptedServerAsync()
        {
            var certificates = _parent.Certificates.SetupServerAuthentication();
            var databaseName = _parent.GetDatabaseName();

            _parent.Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var base64Key = CreateMasterKey(out var buffer);

            var canUseProtect = PlatformDetails.RunningOnPosix == false;

            if (canUseProtect)
            {
                // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
                try
                {
#pragma warning disable CA1416 // Validate platform compatibility
                    ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
#pragma warning restore CA1416 // Validate platform compatibility
                }
                catch (PlatformNotSupportedException)
                {
                    canUseProtect = false;
                }
            }

            if (canUseProtect == false) // fall back to a file
                _parent.Server.ServerStore.Configuration.Security.MasterKeyPath = _parent.GetTempFileName();

            // activate license so we can insert the secret key
            await _parent.Server.ServerStore.EnsureNotPassiveAsync().WaitAsync(TimeSpan.FromSeconds(30));
            await _parent.Server.ServerStore.LicenseManager.TryActivateLicenseAsync(_parent.Server.ThrowOnLicenseActivationFailure).WaitAsync(TimeSpan.FromSeconds(30));

            _parent.Server.ServerStore.PutSecretKey(base64Key, databaseName, overwrite: true);

            return new EncryptServerResult
            {
                Certificates = certificates,
                DatabaseName = databaseName,
                Key = Convert.ToBase64String(buffer)
            };
        }

        public async Task<string> EncryptedClusterAsync(List<RavenServer> nodes, TestCertificatesHolder certificates)
        {
            var databaseName = _parent.GetDatabaseName();

            foreach (var node in nodes)
            {
                _parent.Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, node);

                var base64Key = CreateMasterKey(out _);

                EnsureServerMasterKeyIsSetup(node);

                // activate license so we can insert the secret key
                await _parent.Server.ServerStore.EnsureNotPassiveAsync().WaitAsync(TimeSpan.FromSeconds(30));
                await _parent.Server.ServerStore.LicenseManager.TryActivateLicenseAsync(_parent.Server.ThrowOnLicenseActivationFailure).WaitAsync(TimeSpan.FromSeconds(30));

                node.ServerStore.PutSecretKey(base64Key, databaseName, overwrite: true);
            }

            return databaseName;
        }

        public async Task PutSecretKeyForDatabaseInServerStoreAsync(string databaseName, RavenServer server)
        {
            var base64key = CreateMasterKey(out _);
            var base64KeyClone = new string(base64key.ToCharArray());

            EnsureServerMasterKeyIsSetup(server);

            // activate license so we can insert the secret key
            await _parent.Server.ServerStore.EnsureNotPassiveAsync().WaitAsync(TimeSpan.FromSeconds(30));
            await _parent.Server.ServerStore.LicenseManager.TryActivateLicenseAsync(_parent.Server.ThrowOnLicenseActivationFailure).WaitAsync(TimeSpan.FromSeconds(30));

            server.ServerStore.PutSecretKey(base64key, databaseName, overwrite: true);

            _serverDatabaseToMasterKey.Add((server, databaseName), base64KeyClone);
        }

        public void DeleteSecretKeyForDatabaseFromServerStore(string databaseName, RavenServer server)
        {
            server.ServerStore.DeleteSecretKey(databaseName);
        }

        public string SetupEncryptedDatabase(out TestCertificatesHolder certificates, out byte[] masterKey, [CallerMemberName] string caller = null)
        {
            certificates = _parent.Certificates.SetupServerAuthentication();
            _parent.Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            return SetupEncryptedDatabaseOnNonAuthenticatedServer(out masterKey, caller);
        }

        public string SetupEncryptedDatabaseOnNonAuthenticatedServer(out byte[] masterKey, [CallerMemberName] string caller = null)
        {
            var dbName = _parent.GetDatabaseName(caller);
            string base64Key = CreateMasterKey(out masterKey);
            foreach (var server in _parent.GetServers())
            {
                var copy = new string(base64Key);
                EnsureServerMasterKeyIsSetup(server);
                server.ServerStore.PutSecretKey(copy, dbName, true);
            }
            return dbName;
        }

        public async Task<(string Key, string DatabaseName)> SetupEncryptedDatabaseInCluster(List<RavenServer> nodes, TestCertificatesHolder certificates)
        {
            var databaseName = _parent.GetDatabaseName();
            var base64Key = CreateMasterKey(out _);

            foreach (var node in nodes)
            {
                _parent.Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, node);

                EnsureServerMasterKeyIsSetup(node);
                 
                await _parent.Server.ServerStore.EnsureNotPassiveAsync().WaitAsync(TimeSpan.FromSeconds(30)); // activate license so we can insert the secret key
                await _parent.Server.ServerStore.LicenseManager.TryActivateLicenseAsync(_parent.Server.ThrowOnLicenseActivationFailure).WaitAsync(TimeSpan.FromSeconds(30));

                var key = new string(base64Key);
                node.ServerStore.PutSecretKey(key, databaseName, overwrite: true);
            }

            return (base64Key, databaseName);
        }

        private void EnsureServerMasterKeyIsSetup(RavenServer server)
        {
            var canUseProtect = PlatformDetails.RunningOnPosix == false;

            if (canUseProtect)
            {
                // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
                try
                {
#pragma warning disable CA1416 // Validate platform compatibility
                    ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
#pragma warning restore CA1416 // Validate platform compatibility
                }
                catch (PlatformNotSupportedException)
                {
                    canUseProtect = false;
                }
            }

            if (canUseProtect == false)
            {
                // so we fall back to a file
                if (File.Exists(server.ServerStore.Configuration.Security.MasterKeyPath) == false)
                {
                    server.ServerStore.Configuration.Security.MasterKeyPath = _parent.GetTempFileName();
                }
            }
        }

        public string CreateMasterKey(out byte[] masterKey)
        {
            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }

            masterKey = buffer;

            var base64Key = Convert.ToBase64String(buffer);
            return base64Key;
        }

        public class EncryptServerResult
        {
            public TestCertificatesHolder Certificates { get; set; }

            public string DatabaseName { get; set; }

            public string Key { get; set; }
        }
    }
}
