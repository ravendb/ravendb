using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Raven.Client;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Config.Categories;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.ServerWide
{
    public sealed unsafe class SecretProtection
    {
        public static readonly byte[] EncryptionContext = Encoding.UTF8.GetBytes("Secrets!");

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<SecretProtection>("Server");
        private readonly Lazy<byte[]> _serverMasterKey;
        private readonly SecurityConfiguration _config;
        private const int MaxDeveloperCertificateValidityDurationInMonths = 4;

        public SecretProtection(SecurityConfiguration config)
        {
            _config = config;
            _serverMasterKey = new Lazy<byte[]>(LoadMasterKey);
        }

        private byte[] LoadMasterKey()
        {
            var debug = "<unknown>";
            try
            {
                if (_config.MasterKeyExec != null)
                {
                    debug = _config.MasterKeyExec + " " + _config.MasterKeyExecArguments;
                    return LoadMasterKeyWithExecutable();
                }

                if (_config.MasterKeyPath != null)
                {
                    debug = _config.MasterKeyPath;
                    return LoadMasterKeyFromPath();
                }

                if (PlatformDetails.RunningOnPosix == false)
                    return null;

                var dirpath = Path.Combine(Environment.GetEnvironmentVariable("HOME"), ".ravendb");
                dirpath = Path.GetFullPath(dirpath);
                var filepath = Path.Combine(dirpath, "secret.key");
                debug = filepath;
                var buffer = new byte[(int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes()];
                fixed (byte* pBuf = buffer)
                {
                    if (Directory.Exists(dirpath) == false)
                        Directory.CreateDirectory(dirpath);

                    var fd = Syscall.open(filepath, PerPlatformValues.OpenFlags.O_CREAT | Sparrow.Server.Platform.Posix.OpenFlags.O_RDWR,
                        // octal 01600 - Sticky and only user can read it
                        FilePermissions.S_ISVTX | FilePermissions.S_IRUSR | FilePermissions.S_IWUSR);
                    if (fd == -1)
                    {
                        var err = Marshal.GetLastWin32Error();
                        Syscall.ThrowLastError(err, $"when opening {filepath}");
                    }

                    try
                    {
                        var ret = Syscall.flock(fd, Syscall.FLockOperations.LOCK_EX);
                        if (ret != 0)
                        {
                            var err = Marshal.GetLastWin32Error();
                            Syscall.ThrowLastError(err, $"could not lock {filepath}");
                        }

                        var size = Syscall.lseek64(fd, 0, WhenceFlags.SEEK_END);
                        if (size == -1)
                        {
                            var err = Marshal.GetLastWin32Error();
                            Syscall.ThrowLastError(err, $"could not get size of {filepath}");
                        }

                        if (size == buffer.Length)
                        {
                            byte* pos = pBuf;
                            long amountRead = 0;
                            while (amountRead < buffer.Length)
                            {
                                var read = Syscall.pread(fd, pos, (ulong)(buffer.Length - amountRead), amountRead);
                                pos += read;
                                if (read < 0)
                                {
                                    var err = Marshal.GetLastWin32Error();
                                    Syscall.ThrowLastError(err, $"failed to read {filepath}");
                                }

                                if (read == 0)
                                    break;
                                amountRead += read;
                            }

                            if (amountRead != buffer.Length)
                                throw new FileLoadException($"Failed to read the full key size from {filepath}, expected to read {buffer.Length} but go only {amountRead}");
                        }
                        else // we assume that if the size isn't a key size, then it was never valid and regenerate the key
                        {
                            Sodium.randombytes_buf(pBuf, (UIntPtr)buffer.Length);

                            if (Syscall.ftruncate(fd, IntPtr.Zero) != 0)
                            {
                                var err = Marshal.GetLastWin32Error();
                                Syscall.ThrowLastError(err, $"Failed to truncate {filepath}");
                            }

                            if (Syscall.lseek64(fd, 0, WhenceFlags.SEEK_SET) == -1)
                            {
                                var err = Marshal.GetLastWin32Error();
                                Syscall.ThrowLastError(err, $"Failed to seek to beginning of {filepath}");
                            }

                            var len = buffer.Length;
                            while (len > 0)
                            {
                                var writeAmount = Syscall.write(fd, pBuf, (ulong)buffer.Length);
                                if (writeAmount <= 0) // 0 will be considered as error here
                                {
                                    var err = Marshal.GetLastWin32Error();
                                    Syscall.ThrowLastError(err, $"Failed to write {buffer.Length} bytes into {filepath}, only wrote {len}");
                                }

                                len -= (int)writeAmount;
                            }

                            if (Syscall.FSync(fd) != 0)
                            {
                                var err = Marshal.GetLastWin32Error();
                                Syscall.ThrowLastError(err, $"Failed to FSync {filepath}");
                            }

                            Syscall.FsyncDirectoryFor(filepath);
                        }
                    }
                    finally
                    {
                        if (Syscall.close(fd) != 0)
                        {
                            var err = Marshal.GetLastWin32Error();
                            Syscall.ThrowLastError(err, $"Failed to close the secret key file : {filepath}");
                        }
                    }

                    return buffer;
                }
            }
            catch (Exception e)
            {
                throw new CryptographicException(
                    $"Unable to open the master secret key ({debug}), won't proceed because losing this key will lose access to all user encrypted information. Admin assistance required.",
                    e);
            }
        }

        public byte[] Protect(byte[] secret)
        {
            if (PlatformDetails.RunningOnPosix == false && _config.MasterKeyExec == null && _config.MasterKeyPath == null)
            {
                var tempKey = new byte[(int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes()];
                fixed (byte* pTempKey = tempKey)
                {
                    Sodium.randombytes_buf(pTempKey, Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());

                    var encryptProtectedData = EncryptProtectedData(secret, tempKey);
                    var dpapiEntropy = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_npubbytes());

                    //DPAPI doesn't do AEAD, so we encrypt the data as usual, then encrypt the temp key we use with DPAPI
#pragma warning disable CA1416 // Validate platform compatibility
                    var protectedKey = ProtectedData.Protect(tempKey, dpapiEntropy, DataProtectionScope.CurrentUser);
#pragma warning restore CA1416 // Validate platform compatibility

                    Sodium.sodium_memzero(pTempKey, (UIntPtr)tempKey.Length);

                    var ms = new MemoryStream();
                    var bw = new BinaryWriter(ms);
                    bw.Write(protectedKey.Length);
                    bw.Write(protectedKey);
                    bw.Write(dpapiEntropy.Length);
                    bw.Write(dpapiEntropy);
                    bw.Write(encryptProtectedData.Length);
                    bw.Write(encryptProtectedData);
                    bw.Flush();
                    return ms.ToArray();
                }
            }

            return EncryptProtectedData(secret, _serverMasterKey.Value);
        }

        private static byte[] EncryptProtectedData(byte[] secret, byte[] key)
        {
            var protectedData = new byte[secret.Length + (int)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes() + (int)Sodium.crypto_aead_xchacha20poly1305_ietf_npubbytes()];

            fixed (byte* pContext = EncryptionContext)
            fixed (byte* pSecret = secret)
            fixed (byte* pProtectedData = protectedData)
            fixed (byte* pKey = key)
            {
                var pEntropy = pProtectedData + secret.Length + (int)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes();
                Sodium.randombytes_buf(pEntropy, Sodium.crypto_aead_xchacha20poly1305_ietf_npubbytes());

                var actualKey = stackalloc byte[key.Length];
                if (Sodium.crypto_kdf_derive_from_key(actualKey, (UIntPtr)key.Length, (ulong)SodiumSubKeyId.SecretProtection, pContext, pKey) != 0)
                    throw new InvalidOperationException("Could not derive key for secret encryption");

                ulong cLen;
                var rc = Sodium.crypto_aead_xchacha20poly1305_ietf_encrypt(
                    pProtectedData,
                    &cLen,
                    pSecret,
                    (ulong)secret.Length,
                    null,
                    0,
                    null,
                    pEntropy,
                    actualKey
                );
                if (rc != 0)
                    throw new InvalidOperationException($"Unable to protect secret, rc={rc}");
                Debug.Assert(cLen == (ulong)secret.Length + (ulong)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes());
            }

            return protectedData;
        }

        public byte[] Unprotect(byte[] secret)
        {
            if (PlatformDetails.RunningOnPosix == false && _config.MasterKeyExec == null && _config.MasterKeyPath == null)
            {
                var ms = new MemoryStream(secret);
                var br = new BinaryReader(ms);
                var keyLen = br.ReadInt32();
                var key = br.ReadBytes(keyLen);
                if (key.Length != keyLen)
                    throw new InvalidOperationException("Wrong size for key buffer: " + key.Length);

                var entrophyLen = br.ReadInt32();
                if (entrophyLen != (int)Sodium.crypto_aead_xchacha20poly1305_ietf_npubbytes())
                    throw new InvalidOperationException("Wrong size for nonce len: " + entrophyLen);

                var entropy = br.ReadBytes(entrophyLen);
                if (entropy.Length != (int)Sodium.crypto_aead_xchacha20poly1305_ietf_npubbytes())
                    throw new InvalidOperationException("Wrong size for nonce buffer: " + entropy.Length);

                var dataLen = br.ReadInt32();
                var data = br.ReadBytes(dataLen);
                if (data.Length != dataLen)
                    throw new InvalidOperationException("Wrong size for data buffer: " + entropy.Length + " but expected " + dataLen);

#pragma warning disable CA1416 // Validate platform compatibility
                var plainKey = ProtectedData.Unprotect(key, entropy, DataProtectionScope.CurrentUser);
#pragma warning restore CA1416 // Validate platform compatibility
                try
                {
                    return DecryptProtectedData(data, plainKey);
                }
                finally
                {
                    Sodium.ZeroBuffer(plainKey);
                }
            }

            return DecryptProtectedData(secret, _serverMasterKey.Value);
        }

        private static byte[] DecryptProtectedData(byte[] secret, byte[] key)
        {
            // here we'll throw if the size of the secret buffer is too small
            var unprotectedData = new byte[secret.Length - (int)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes()
                                                         - (int)Sodium.crypto_aead_xchacha20poly1305_ietf_npubbytes()];

            fixed (byte* pContext = EncryptionContext)
            fixed (byte* pSecret = secret)
            fixed (byte* pUnprotectedData = unprotectedData)
            fixed (byte* pKey = key)
            {
                var actualSecretLen = secret.Length - (int)Sodium.crypto_aead_xchacha20poly1305_ietf_npubbytes();
                var pEntropy = pSecret + actualSecretLen;

                var actualKey = stackalloc byte[key.Length];
                if (Sodium.crypto_kdf_derive_from_key(actualKey, (UIntPtr)key.Length, (ulong)SodiumSubKeyId.SecretProtection, pContext, pKey) != 0)
                    throw new InvalidOperationException("Could not derive key for secret decryption");

                ulong mLen;
                var rc = Sodium.crypto_aead_xchacha20poly1305_ietf_decrypt(
                    pUnprotectedData,
                    &mLen,
                    null,
                    pSecret,
                    (ulong)actualSecretLen,
                    null,
                    0,
                    pEntropy,
                    actualKey
                );

                if (rc != 0)
                    throw new InvalidOperationException($"Unable to unprotect secret, rc={rc}");

                Debug.Assert(mLen == (ulong)actualSecretLen - (ulong)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes());
            }

            return unprotectedData;
        }

        private static void ValidateExpiration(string source, X509Certificate2 loadedCertificate, LicenseType licenseType, bool throwOnExpired = true, SetupProgressAndResult progress = null)
        {
            if (loadedCertificate.NotAfter < DateTime.UtcNow)
            {
                string msg = $"The provided certificate '{loadedCertificate.FriendlyName}' from {source} is expired! Thumbprint: {loadedCertificate.Thumbprint}, Expired on: {loadedCertificate.NotAfter}";
                if (Logger.IsOperationsEnabled)
                    Logger.Operations(msg);

                progress?.AddError(msg);
                if (throwOnExpired)
                    throw new InvalidOperationException(msg);
            }


            if (licenseType == LicenseType.Developer)
            {
                // Do not allow long range certificates in developer mode.
                if (loadedCertificate.NotAfter > DateTime.UtcNow.AddMonths(MaxDeveloperCertificateValidityDurationInMonths))
                {
                    const string msg = "The server certificate expiration date is more than 4 months from now. " +
                                       "This is not allowed when using Developer license. " +
                                       "Developer license is not allowed for production use. " +
                                       "Either switch the license or use a short term certificate.";

                    if (Logger.IsOperationsEnabled)
                        Logger.Operations(msg);

                    progress?.AddError(msg);
                    throw new InvalidOperationException(msg);
                }
            }
        }

        public static CertificateUtils.CertificateHolder ValidateCertificateAndCreateCertificateHolder(string source,
            X509Certificate2 serverCertificate,
            byte[] rawBytes,
            string password,
            LicenseType licenseType,
            bool validateCertKeyUsages,
            SetupProgressAndResult progress = null)
        {
            AsymmetricAlgorithm privateKey = ValidateServerCertificate(source, serverCertificate, rawBytes, password, licenseType, validateCertKeyUsages, progress);

            return new CertificateUtils.CertificateHolder(serverCertificate, privateKey);
        }

        public static AsymmetricAlgorithm ValidateServerCertificate(string source,
            X509Certificate2 loadedCertificate,
            byte[] rawBytes,
            string password,
            LicenseType licenseType,
            bool validateCertKeyUsages,
            SetupProgressAndResult progress = null)
        {
            ValidateExpiration(source, loadedCertificate, licenseType, progress: progress);
            
            AsymmetricAlgorithm privateKey = null;
            if (PlatformDetails.RunningOnMacOsx)
            {
                ValidatePrivateKeyOnMacOs(source, loadedCertificate, out privateKey);
            }
            else
            {
                ValidatePrivateKey(source, password, rawBytes, out privateKey, progress);
            }
            
            ValidateServerKeyUsages(source, loadedCertificate, validateCertKeyUsages, progress);
            return privateKey;
        }

        public static void ValidateServerKeyUsages(string source, X509Certificate2 loadedCertificate, bool validateKeyUsages, SetupProgressAndResult progress = null)
        {
            var serverCert = false;
            var keyUsages = false;

            foreach (var extension in loadedCertificate.Extensions)
            {
                if (extension is X509KeyUsageExtension kue)
                {
                    if (kue.KeyUsages.HasFlag(X509KeyUsageFlags.DigitalSignature))
                        keyUsages = true;
                }

                if (extension is X509EnhancedKeyUsageExtension ekue) //Enhanced Key Usage extension
                {
                    foreach (var usage in ekue.EnhancedKeyUsages)
                    {
                        switch (usage.Value)
                        {
                            case Constants.Certificates.ServerAuthenticationOid:
                                serverCert = true;
                                break;
                        }
                    }
                }
            }

            var shouldThrow = serverCert == false;
            if (validateKeyUsages && keyUsages == false)
                shouldThrow = true;

            if (shouldThrow == false)
                return;

            var sb = new StringBuilder($"Server certificate {loadedCertificate.FriendlyName} from {source} must be defined with:");

            if (validateKeyUsages && keyUsages == false)
            {
                sb.AppendLine("- Key Usage: DigitalSignature");
            }

            sb.AppendLine($"- Enhanced Key Usage: Server Authentication (Oid {Constants.Certificates.ServerAuthenticationOid})");

            var msg = sb.ToString();

            if (Logger.IsOperationsEnabled)
                Logger.Operations(msg);
            progress?.AddInfo(msg);

            throw new CryptographicException(msg);
        }

        public static bool HasCertificateClientAuthEnhancedKeyUsage(X509Certificate2 certificate)
        {
            if (certificate == null)
                return false;

            foreach (var extension in certificate.Extensions)
            {
                if (extension is X509EnhancedKeyUsageExtension ekue) //Enhanced Key Usage extension
                {
                    foreach (var usage in ekue.EnhancedKeyUsages)
                    {
                        switch (usage.Value)
                        {
                            case Constants.Certificates.ClientAuthenticationOid:
                                return true;
                        }
                    }
                }
            }

            return false;
        }
        
        public static bool HasCertificateServerAuthEnhancedKeyUsage(X509Certificate2 certificate)
        {
            if (certificate == null)
                return false;
            
            foreach (var extension in certificate.Extensions)
            {
                if (extension is X509EnhancedKeyUsageExtension ekue) //Enhanced Key Usage extension
                {
                    foreach (var usage in ekue.EnhancedKeyUsages)
                    {
                        switch (usage.Value)
                        {
                            case Constants.Certificates.ServerAuthenticationOid:
                                return true;
                        }
                    }
                }
            }

            return false;
        }

#if !RVN

        public (X509Certificate2 Certificate, AsymmetricAlgorithm PrivateKey) LoadCertificateWithExecutable(string executable,
            string args,
            LicenseType licenseType,
            bool certificateValidationKeyUsages)
        {
            Process process;

            var startInfo = new ProcessStartInfo
            {
                FileName = executable,
                Arguments = args,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            var sw = Stopwatch.StartNew();

            try
            {
                process = Process.Start(startInfo);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to get certificate by executing {executable} {args}. Failed to start process.", e);
            }

            var ms = new MemoryStream();
            var readErrors = process.StandardError.ReadToEndAsync();
            var readStdOut = process.StandardOutput.BaseStream.CopyToAsync(ms);
            var timeoutInMs = (int)_config.CertificateExecTimeout.AsTimeSpan.TotalMilliseconds;

            string GetStdError()
            {
                try
                {
                    return readErrors.Result;
                }
                catch
                {
                    return "Unable to get stderr";
                }
            }

            if (process.WaitForExit(timeoutInMs) == false)
            {
                process.Kill();
                throw new InvalidOperationException($"Unable to get certificate by executing {executable} {args}, waited for {timeoutInMs} ms but the process didn't exit. Stderr: {GetStdError()}");
            }

            try
            {
                readStdOut.Wait(timeoutInMs);
                readErrors.Wait(timeoutInMs);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to get certificate by executing {executable} {args}, waited for {timeoutInMs} ms but the process didn't exit. Stderr: {GetStdError()}", e);
            }

            if (Logger.IsOperationsEnabled)
            {
                var errors = GetStdError();
                Logger.Operations($"Executing {executable} {args} took {sw.ElapsedMilliseconds:#,#;;0} ms");
                if (string.IsNullOrWhiteSpace(errors) == false)
                    Logger.Operations($"Executing {executable} {args} finished with exit code: {process.ExitCode}. Errors: {errors}");
            }

            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException($"Unable to get certificate by executing {executable} {args}, the exit code was {process.ExitCode}. Stderr: {GetStdError()}");
            }

            var rawData = ms.ToArray();
            X509Certificate2 loadedCertificate;
            try
            {
                // may need to send this over the cluster, so use exportable here
                loadedCertificate = CertificateLoaderUtil.CreateCertificate(rawData, (string)null, CertificateLoaderUtil.FlagsForExport);
                ValidateExpiration(executable, loadedCertificate, licenseType, throwOnExpired: false);
                ValidatePrivateKey(executable, null, rawData, out var privateKey);
                ValidateServerKeyUsages(executable, loadedCertificate, certificateValidationKeyUsages);
                
                return (loadedCertificate, privateKey);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Got invalid certificate via {executable} {args}", e);
            }
        }

        public void NotifyExecutableOfCertificateChange(string executable, string args, string newCertificateBase64)
        {
            Process process;

            var startInfo = new ProcessStartInfo
            {
                FileName = executable,
                Arguments = args,
                UseShellExecute = false,
                RedirectStandardError = true,
                RedirectStandardInput = true,
                CreateNoWindow = true
            };

            var sw = Stopwatch.StartNew();

            try
            {
                process = Process.Start(startInfo);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to execute {executable} {args}. Failed to start process.", e);
            }

            process.StandardInput.WriteLine(newCertificateBase64);
            process.StandardInput.Flush();

            var readErrors = process.StandardError.ReadToEndAsync();

            string GetStdError()
            {
                try
                {
                    return readErrors.Result;
                }
                catch
                {
                    return "Unable to get stderr";
                }
            }

            if (process.WaitForExit((int)_config.CertificateExecTimeout.AsTimeSpan.TotalMilliseconds) == false)
            {
                process.Kill();
                throw new InvalidOperationException($"Unable to execute {executable} {args}, waited for {_config.CertificateExecTimeout} ms but the process didn't exit. Stderr: {GetStdError()}");
            }

            try
            {
                readErrors.Wait(_config.CertificateExecTimeout.AsTimeSpan);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to execute {executable} {args}, waited for {_config.CertificateExecTimeout} ms but the process didn't exit. Stderr: {GetStdError()}", e);
            }

            if (Logger.IsOperationsEnabled)
            {
                var errors = GetStdError();
                Logger.Operations($"Executing {executable} {args} took {sw.ElapsedMilliseconds:#,#;;0} ms");
                if (string.IsNullOrWhiteSpace(errors) == false)
                    Logger.Operations($"Executing {executable} {args} finished with exit code: {process.ExitCode}. Errors: {errors}");
            }

            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException($"Unable to execute {executable} {args}, the exit code was {process.ExitCode}. Stderr: {GetStdError()}");
            }
        }

#endif

        private byte[] LoadMasterKeyWithExecutable()
        {
            Process process;

            var startInfo = new ProcessStartInfo
            {
                FileName = _config.MasterKeyExec,
                Arguments = _config.MasterKeyExecArguments,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            var sw = Stopwatch.StartNew();

            try
            {
                process = Process.Start(startInfo);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to get master key by executing {_config.MasterKeyExec} {_config.MasterKeyExecArguments}. Failed to start process.", e);
            }

            var ms = new MemoryStream();
            var readErrors = process.StandardError.ReadToEndAsync();
            var readStdOut = process.StandardOutput.BaseStream.CopyToAsync(ms);

            string GetStdError()
            {
                try
                {
                    return readErrors.Result;
                }
                catch
                {
                    return "Unable to get stdout";
                }
            }

            if (process.WaitForExit((int)_config.MasterKeyExecTimeout.AsTimeSpan.TotalMilliseconds) == false)
            {
                process.Kill();

                throw new InvalidOperationException($"Unable to get master key by executing {_config.MasterKeyExec} {_config.MasterKeyExecArguments}, waited for {_config.MasterKeyExecTimeout} ms but the process didn't exit. Stderr: {GetStdError()}");
            }

            try
            {
                readStdOut.Wait(_config.MasterKeyExecTimeout.AsTimeSpan);
                readErrors.Wait(_config.MasterKeyExecTimeout.AsTimeSpan);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to get master key by executing {_config.MasterKeyExec} {_config.MasterKeyExecArguments}, waited for {_config.MasterKeyExecTimeout} ms but the process didn't exit. Stderr: {GetStdError()}", e);
            }

            if (Logger.IsOperationsEnabled)
            {
                var errors = GetStdError();
                Logger.Operations($"Executing {_config.MasterKeyExec} {_config.MasterKeyExecArguments} took {sw.ElapsedMilliseconds:#,#;;0} ms. Stderr: {errors}");
            }

            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException($"Unable to get master key by executing {_config.MasterKeyExec} {_config.MasterKeyExecArguments}, the exit code was {process.ExitCode}. Stderr: {GetStdError()}");
            }

            var rawData = ms.ToArray();

            var expectedKeySize = (int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();
            if (rawData.Length != expectedKeySize)
            {
                throw new InvalidOperationException($"Got wrong master key after executing {_config.MasterKeyExec} {_config.MasterKeyExecArguments}, the size of the key must be {expectedKeySize * 8} bits, but was {rawData.Length * 8} bits.");
            }

            return rawData;
        }

        public static void AddCertificateChainToTheUserCertificateAuthorityStoreAndCleanExpiredCerts(X509Certificate2 loadedCertificate, byte[] rawBytes, string password, SetupProgressAndResult progress = null)
        {
            // we have to add all the certs in the pfx file provides to the CA store for the current user
            // to avoid a remote call on any incoming connection by the SslStream infrastructure
            // see: https://github.com/dotnet/corefx/issues/26061

            var collection = new X509Certificate2Collection();

            if (string.IsNullOrEmpty(password))
                CertificateLoaderUtil.Import(collection, rawBytes);
            else
                CertificateLoaderUtil.Import(collection, rawBytes, password);

            var storeName = PlatformDetails.RunningOnMacOsx ? StoreName.My : StoreName.CertificateAuthority;
            using (var userIntermediateStore = new X509Store(storeName, StoreLocation.CurrentUser,
                       System.Security.Cryptography.X509Certificates.OpenFlags.ReadWrite))
            {
                foreach (var cert in collection)
                {
                    if (cert.Thumbprint == loadedCertificate.Thumbprint)
                        continue;

                    var results = userIntermediateStore.Certificates.Find(X509FindType.FindByThumbprint, cert.Thumbprint, false);
                    if (results.Count == 0)
                        userIntermediateStore.Add(cert);
                }

                // We had a problem where we didn't cleanup the user store in Linux (~/.dotnet/corefx/cryptography/x509stores/ca)
                // and it exploded with thousands of certificates. This caused ssl handshakes to fail on that machine, because it would timeout when
                // trying to match one of these certs to validate the chain.

                IEnumerable<X509Certificate2> existingCerts;
                if (string.IsNullOrEmpty(loadedCertificate.SubjectName.Name))
                {
                    existingCerts = userIntermediateStore.Certificates.Where(x => x.GetDisplayName() == loadedCertificate.GetDisplayName()).ToList();;
                }
                else
                {
                var cnValue = loadedCertificate.SubjectName.Name.StartsWith("CN=", StringComparison.OrdinalIgnoreCase)
                    ? loadedCertificate.SubjectName.Name.Substring(3)
                    : loadedCertificate.SubjectName.Name;
                    existingCerts = userIntermediateStore.Certificates.Find(X509FindType.FindBySubjectName, cnValue, false);
                }

                var utcNow = DateTime.UtcNow;
                foreach (var c in existingCerts)
                {
                    if (c.NotAfter.ToUniversalTime() > utcNow && c.NotBefore.ToUniversalTime() < utcNow)
                        continue;

                    // Remove all expired certs which have the same subject name as our own
                    var chain = new X509Chain();
                    chain.ChainPolicy.DisableCertificateDownloads = true;

                    chain.Build(c);

                    foreach (var element in chain.ChainElements)
                    {
                        if (element.Certificate.NotAfter.ToUniversalTime() > utcNow && element.Certificate.NotBefore.ToUniversalTime() < utcNow)
                            continue;
                        try
                        {
                            userIntermediateStore.Remove(element.Certificate);
                        }
                        catch (CryptographicException e)
                        {
                            var msg = $"Tried to clean expired certificates from the OS user intermediate store but got an exception when removing a certificate with subject name '{element.Certificate.SubjectName.Name}' and thumbprint '{element.Certificate.Thumbprint}'.";

                            if (Logger is { IsInfoEnabled: true })
                                Logger.Info(msg, e);
                            progress?.AddError(msg, e);
                        }
                    }
                }
            }
        }

        public (X509Certificate2 Certificate, AsymmetricAlgorithm PrivateKey) LoadCertificateFromPath(string path,
            string password,
            LicenseType licenseType,
            bool certificateValidationKeyUsages)
        {
            try
            {
                path = Path.Combine(AppContext.BaseDirectory, path);
                var rawData = File.ReadAllBytes(path);

                // we need to load it as exportable because we might need to send it over the cluster
                var loadedCertificate = CertificateLoaderUtil.CreateCertificate(rawData, password, CertificateLoaderUtil.FlagsForExport);

                ValidateExpiration(path, loadedCertificate, licenseType, throwOnExpired: false);

                AsymmetricAlgorithm privateKey = null;
                if (PlatformDetails.RunningOnMacOsx)
                {
                    ValidatePrivateKeyOnMacOs(path, loadedCertificate, out privateKey);
                }
                else
                {
                    ValidatePrivateKey(path, password, rawData, out privateKey);
                }
                
                ValidateServerKeyUsages(path, loadedCertificate, certificateValidationKeyUsages);

                return (loadedCertificate, privateKey);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Could not load certificate file {path}", e);
            }
        }

        public static void ValidateCertificateBeforeReplacement(X509Certificate2 certificate, string password, LicenseType licenseType, bool certificateValidationKeyUsages)
        {
            ValidateExpiration("ValidateCertificateBeforeReplacement", certificate, licenseType, throwOnExpired: true);

            if (PlatformDetails.RunningOnMacOsx)
            {
                // macOS AppleCrypto blocks exporting ephemeral private keys to PFX, 
                // We validate the private key's presence directly in memory instead.
                ValidatePrivateKeyOnMacOs("ValidateCertificateBeforeReplacement", certificate, out var pk);
                pk?.Dispose();
            }
            else
            {
                // On Windows and Linux, proceed with the standard export-based validation
                ValidatePrivateKey("ValidateCertificateBeforeReplacement", password, certificate.Export(X509ContentType.Pkcs12), out var pk);
                pk?.Dispose();
            }

            ValidateServerKeyUsages("ValidateCertificateBeforeReplacement", certificate, certificateValidationKeyUsages);
        }
        
        internal static void ValidatePrivateKeyOnMacOs(string source, X509Certificate2 certificate, out AsymmetricAlgorithm pk, SetupProgressAndResult progress = null)
        {
            // Attempt to get the private key directly from memory
            pk = certificate.GetRSAPrivateKey() ?? (AsymmetricAlgorithm)certificate.GetECDsaPrivateKey();

            // If the certificate is explicitly marked as not having a key, 
            // or if the key extraction failed/returned null, throw the exact expected exception.
            if (certificate.HasPrivateKey == false || pk == null)
            {
                ThrowCryptographicException(source, progress);
            }
        }

        internal static void ValidatePrivateKey(string source, string certificatePassword, byte[] rawData, out AsymmetricAlgorithm pk, SetupProgressAndResult progress = null)
        {
            pk = null;
            var certificate = CertificateLoaderUtil.CreateCertificate(rawData, certificatePassword, X509KeyStorageFlags.PersistKeySet);

            // Get the private key.
            pk = certificate.GetRSAPrivateKey();

            if (pk == null)
            {
                ThrowCryptographicException(source, progress);
            }
        }

        private static void ThrowCryptographicException(string source, SetupProgressAndResult progress = null)
        {
            string msg = "Unable to find the private key in the provided certificate from " + source;
            if (Logger.IsOperationsEnabled)
                Logger.Operations(msg);
            progress?.AddInfo(msg);
            throw new CryptographicException(msg);
        }


        internal static void ValidateExpiration(string source, LicenseType currentLicenseType, LicenseType licenseType, X509Certificate2 certificate, DateTime certificateNotBefore, DateTime certificateNotAfter)
        {

            if (licenseType != LicenseType.Developer)
                return;
            if (certificate == null)
                return;

            var certificateMaxDuration = certificateNotBefore.AddMonths(MaxDeveloperCertificateValidityDurationInMonths);
            string msg;

            if (certificateNotAfter > certificateMaxDuration)
            {
                msg = $"The server certificate total duration is greater than {MaxDeveloperCertificateValidityDurationInMonths} months." +
                      $"This is not allowed when using {LicenseType.Developer} license." +
                      $"Use short term certificate duration for up to {MaxDeveloperCertificateValidityDurationInMonths}";

                throw new InvalidOperationException(msg);
            }

            // Do not allow long range certificates in developer mode if the certificate uploaded from another license type.
            if (certificateNotAfter > DateTime.UtcNow.AddMonths(MaxDeveloperCertificateValidityDurationInMonths))
            {
                msg = $"The server certificate expiration date is more than {MaxDeveloperCertificateValidityDurationInMonths} months from now. " +
                      $"This is not allowed when trying to change the license from {currentLicenseType} the {LicenseType.Developer} license. " +
                      "Use short term certificate before changing the license";

                throw new InvalidOperationException(msg);
            }
        }

        public byte[] LoadMasterKeyFromPath()
        {
            try
            {
                var key = File.ReadAllBytes(_config.MasterKeyPath);
                var expectedKeySize = (int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();

                // we require that the key will exists (so admin will generate proper permissions)
                // but if the size is zero, we'll generate a random key and save it to the specified
                // file

                if (key.Length == 0)
                {
                    key = Sodium.GenerateRandomBuffer(expectedKeySize);
                    File.WriteAllBytes(_config.MasterKeyPath, key);
                }

                if (key.Length != expectedKeySize)
                {
                    throw new InvalidOperationException($"The size of the key must be {expectedKeySize * 8} bits, but was {key.Length * 8} bits.");
                }

                return key;
            }
            catch (Exception e)
            {
                throw new CryptographicException($"Unable to open the master secret key at {_config.MasterKeyPath}, won't proceed because losing this key will lose access to all user encrypted information. Admin assistance required.", e);
            }
        }
    }
}

