using System;
using System.IO;
using Raven.Server.Config.Categories;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;
using Voron;
using Voron.Impl.Compaction;

namespace rvn
{
    internal class OfflineOperations
    {
        private const string SecretKeyEncrypted = "secret.key.encrypted";
        
        public static string GetKey(string srcDir)
        {
            var masterKey = Sodium.GenerateMasterKey();
            var dstDir = Path.Combine(Path.GetDirectoryName(srcDir), "Temp.Encryption");

            var srcOptions = StorageEnvironmentOptions.ForPath(srcDir);
            var dstOptions = StorageEnvironmentOptions.ForPath(dstDir);

            dstOptions.MasterKey = masterKey;

            var entropy = Sodium.GenerateRandomBuffer(32); // 256-bit
            var protect = new SecretProtection(new SecurityConfiguration()).Protect(masterKey, entropy);

            StorageCompaction.Execute(srcOptions, (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)dstOptions);

            using (var f = File.OpenWrite(Path.Combine(dstDir, SecretKeyEncrypted)))
            {
                f.Write(protect, 0, protect.Length);
                f.Write(entropy, 0, entropy.Length);
                f.Flush();
            }

            return $"GetKey: {Path.Combine(dstDir, SecretKeyEncrypted)} Created Successfully";
        }

        public static string PutKey(string destDir)
        {
            var base64Key = RecoverServerStoreKey(destDir);
            var entropy = Sodium.GenerateRandomBuffer(32); // 256-bit
            var secret = Convert.FromBase64String(base64Key);
            var protect = new SecretProtection(new SecurityConfiguration()).Protect(secret, entropy);

            using (var f = File.OpenWrite(Path.Combine(destDir, SecretKeyEncrypted)))
            {
                f.Write(protect, 0, protect.Length);
                f.Write(entropy, 0, entropy.Length);
                f.Flush();
            }

            return $"PutKey: {Path.Combine(destDir, SecretKeyEncrypted)} Created Successfully";
        }

        public static string InitKeys()
        {
            return "InitKeys is not implemented";
        }

        public static string Encrypt(string srcDir)
        {
            var masterKey = Sodium.GenerateMasterKey();
            var dstDir = Path.Combine(Path.GetDirectoryName(srcDir), "Temp.Encryption");

            var srcOptions = StorageEnvironmentOptions.ForPath(srcDir);
            var dstOptions = StorageEnvironmentOptions.ForPath(dstDir);

            dstOptions.MasterKey = masterKey;

            var entropy = Sodium.GenerateRandomBuffer(32); // 256-bit
            var protect = new SecretProtection(new SecurityConfiguration()).Protect(masterKey, entropy);

            StorageCompaction.Execute(srcOptions, (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)dstOptions);

            using (var f = File.OpenWrite(Path.Combine(dstDir, SecretKeyEncrypted)))
            {
                f.Write(protect, 0, protect.Length);
                f.Write(entropy, 0, entropy.Length);
                f.Flush();
            }

            IOExtensions.DeleteDirectory(srcDir);
            Directory.Move(dstDir, srcDir);

            return $"Encrypt: {Path.Combine(dstDir, SecretKeyEncrypted)} Created Successfully";
        }

        public static string Decrypt(string srcDir)
        {
            var dstDir = Path.Combine(Path.GetDirectoryName(srcDir), "Temp.Decryption");
            var bytes = File.ReadAllBytes(Path.Combine(srcDir, SecretKeyEncrypted));
            var secret = new byte[bytes.Length - 32];
            var entropy = new byte[32];
            Array.Copy(bytes, 0, secret, 0, bytes.Length - 32);
            Array.Copy(bytes, bytes.Length - 32, entropy, 0, 32);

            var srcOptions = StorageEnvironmentOptions.ForPath(srcDir);
            var dstOptions = StorageEnvironmentOptions.ForPath(dstDir);

            srcOptions.MasterKey = new SecretProtection(new SecurityConfiguration()).Unprotect(secret, entropy);

            StorageCompaction.Execute(srcOptions, (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)dstOptions);

            IOExtensions.DeleteDirectory(srcDir);
            Directory.Move(dstDir, srcDir);

            return $"Decrypt: {Path.Combine(dstDir, SecretKeyEncrypted)} Created Successfully";
        }

        public static string Trust(string key, string tag)
        {
            return "Trust is not implemented";
        }

        private static string RecoverServerStoreKey(string srcDir)
        {
            var keyPath = Path.Combine(srcDir, SecretKeyEncrypted);
            if (File.Exists(keyPath) == false)
                throw new IOException("File not exists:" + keyPath);

            var buffer = File.ReadAllBytes(keyPath);
            var secret = new byte[buffer.Length - 32];
            var entropy = new byte[32];
            Array.Copy(buffer, 0, secret, 0, buffer.Length - 32);
            Array.Copy(buffer, buffer.Length - 32, entropy, 0, 32);

            var key = new SecretProtection(new SecurityConfiguration()).Unprotect(secret, entropy);
            return Convert.ToBase64String(key);
        }


    }
}
