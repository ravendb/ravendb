using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server;
using Sparrow.Platform;
using Xunit;

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

        public string EncryptedServer(out TestCertificatesHolder certificates, out string databaseName)
        {
            certificates = _parent.Certificates.SetupServerAuthentication();
            databaseName = _parent.GetDatabaseName();
            _parent.Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }

            var base64Key = Convert.ToBase64String(buffer);

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

            Assert.True(_parent.Server.ServerStore.EnsureNotPassiveAsync().Wait(TimeSpan.FromSeconds(30))); // activate license so we can insert the secret key
            Assert.True(_parent.Server.ServerStore.LicenseManager.TryActivateLicenseAsync(_parent.Server.ThrowOnLicenseActivationFailure).Wait(TimeSpan.FromSeconds(30))); // activate license so we can insert the secret key
            _parent.Server.ServerStore.PutSecretKey(base64Key, databaseName, overwrite: true);

            return Convert.ToBase64String(buffer);
        }

        public void EncryptedCluster(List<RavenServer> nodes, TestCertificatesHolder certificates, out string databaseName)
        {
            databaseName = _parent.GetDatabaseName();

            foreach (var node in nodes)
            {
                _parent.Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, node);

                var base64Key = CreateMasterKey(out _);

                EnsureServerMasterKeyIsSetup(node);

                Assert.True(node.ServerStore.EnsureNotPassiveAsync().Wait(TimeSpan.FromSeconds(30))); // activate license so we can insert the secret key
                Assert.True(node.ServerStore.LicenseManager.TryActivateLicenseAsync(_parent.Server.ThrowOnLicenseActivationFailure).Wait(TimeSpan.FromSeconds(30))); // activate license so we can insert the secret key

                node.ServerStore.PutSecretKey(base64Key, databaseName, overwrite: true);
            }
        }

        public void PutSecretKeyForDatabaseInServerStore(string databaseName, RavenServer server)
        {
            var base64key = CreateMasterKey(out _);
            var base64KeyClone = new string(base64key.ToCharArray());

            EnsureServerMasterKeyIsSetup(server);

            Assert.True(server.ServerStore.EnsureNotPassiveAsync().Wait(TimeSpan.FromSeconds(30))); // activate license so we can insert the secret key
            Assert.True(server.ServerStore.LicenseManager.TryActivateLicenseAsync(_parent.Server.ThrowOnLicenseActivationFailure).Wait(TimeSpan.FromSeconds(30))); // activate license so we can insert the secret key

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
            var dbName = _parent.GetDatabaseName(caller);
            _parent.Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            string base64Key = CreateMasterKey(out masterKey);

            EnsureServerMasterKeyIsSetup(_parent.Server);

            _parent.Server.ServerStore.PutSecretKey(base64Key, dbName, true);
            return dbName;
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
    }
}
