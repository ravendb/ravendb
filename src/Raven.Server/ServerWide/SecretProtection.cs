﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Org.BouncyCastle.Asn1;
using Org.BouncyCastle.Asn1.Pkcs;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.Utilities;
using Org.BouncyCastle.Utilities.Collections;
using Org.BouncyCastle.Utilities.Encoders;
using Org.BouncyCastle.X509;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Config.Categories;
using Raven.Server.Logging;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.ServerWide
{
    public sealed unsafe class SecretProtection
    {
        public static readonly byte[] EncryptionContext = Encoding.UTF8.GetBytes("Secrets!");

        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<SecretProtection>();
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
                if (Logger.IsErrorEnabled)
                    Logger.Error(msg);

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

                    if (Logger.IsErrorEnabled)
                        Logger.Error(msg);

                    progress?.AddError(msg);
                    throw new InvalidOperationException(msg);
                }
            }
        }

        public static CertificateUtils.CertificateHolder ValidateCertificateAndCreateCertificateHolder(string source, X509Certificate2 loadedCertificate, byte[] rawBytes, string password, LicenseType licenseType, bool validateCertKeyUsages, SetupProgressAndResult progress = null)
        {
            ValidateExpiration(source, loadedCertificate, licenseType, progress: progress);

            ValidatePrivateKey(source, password, rawBytes, out var privateKey, progress);

            ValidateKeyUsages(source, loadedCertificate, validateCertKeyUsages, progress);

            AddCertificateChainToTheUserCertificateAuthorityStoreAndCleanExpiredCerts(loadedCertificate, rawBytes, password, progress);

            return new CertificateUtils.CertificateHolder(loadedCertificate, privateKey, Convert.ToBase64String(loadedCertificate.Export(X509ContentType.Cert)));
        }

        public static void ValidateKeyUsages(string source, X509Certificate2 loadedCertificate, bool validateKeyUsages, SetupProgressAndResult progress = null)
        {
            var clientCert = false;
            var serverCert = false;
            var keyUsages = false;

            foreach (var extension in loadedCertificate.Extensions)
            {
                if (extension is X509KeyUsageExtension kue)
                {
                    if (kue.KeyUsages.HasFlag(X509KeyUsageFlags.DigitalSignature) && kue.KeyUsages.HasFlag(X509KeyUsageFlags.KeyEncipherment))
                        keyUsages = true;
                }

                if (extension is X509EnhancedKeyUsageExtension ekue) //Enhanced Key Usage extension
                {
                    foreach (var usage in ekue.EnhancedKeyUsages)
                    {
                        switch (usage.Value)
                        {
                            case "1.3.6.1.5.5.7.3.2":
                                clientCert = true;
                                break;
                            case "1.3.6.1.5.5.7.3.1":
                                serverCert = true;
                                break;
                        }
                    }
                }
            }

            var shouldThrow = clientCert == false || serverCert == false;
            if (validateKeyUsages && keyUsages == false)
                shouldThrow = true;

            if (shouldThrow == false)
                return;

            var sb = new StringBuilder($"Server certificate {loadedCertificate.FriendlyName} from {source} must be defined with:");

            if (validateKeyUsages && keyUsages == false)
            {
                sb.AppendLine("- Key Usage: DigitalSignature");
                sb.AppendLine("- Key Usage: KeyEncipherment");
            }

            sb.AppendLine("- Enhanced Key Usage: Client Authentication (Oid 1.3.6.1.5.5.7.3.2)");
            sb.AppendLine("- Enhanced Key Usage: Server Authentication (Oid 1.3.6.1.5.5.7.3.1)");

            var msg = sb.ToString();

            if (Logger.IsErrorEnabled)
                Logger.Error(msg);
            progress?.AddInfo(msg);

            throw new EncryptionException(msg);
        }

#if !RVN


        public CertificateUtils.CertificateHolder LoadCertificateWithExecutable(string executable, string args, LicenseType licenseType, bool certificateValidationKeyUsages)
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
                throw new InvalidOperationException($"Unable to get certificate by executing {executable} {args}, waited for {_config.CertificateExecTimeout} ms but the process didn't exit. Stderr: {GetStdError()}");
            }

            try
            {
                readStdOut.Wait(_config.CertificateExecTimeout.AsTimeSpan);
                readErrors.Wait(_config.CertificateExecTimeout.AsTimeSpan);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to get certificate by executing {executable} {args}, waited for {_config.CertificateExecTimeout} ms but the process didn't exit. Stderr: {GetStdError()}", e);
            }

            if (Logger.IsErrorEnabled)
            {
                var errors = GetStdError();
                Logger.Error($"Executing {executable} {args} took {sw.ElapsedMilliseconds:#,#;;0} ms");
                if (!string.IsNullOrWhiteSpace(errors))
                    Logger.Error($"Executing {executable} {args} finished with exit code: {process.ExitCode}. Errors: {errors}");
            }

            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException($"Unable to get certificate by executing {executable} {args}, the exit code was {process.ExitCode}. Stderr: {GetStdError()}");
            }

            var rawData = ms.ToArray();
            X509Certificate2 loadedCertificate;
            AsymmetricKeyEntry privateKey;
            try
            {
                // may need to send this over the cluster, so use exportable here
                loadedCertificate = CertificateLoaderUtil.CreateCertificate(rawData, (string)null, CertificateLoaderUtil.FlagsForExport);
                ValidateExpiration(executable, loadedCertificate, licenseType, throwOnExpired: false);
                ValidatePrivateKey(executable, null, rawData, out privateKey);
                ValidateKeyUsages(executable, loadedCertificate, certificateValidationKeyUsages);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Got invalid certificate via {executable} {args}", e);
            }

            return new CertificateUtils.CertificateHolder(loadedCertificate, privateKey, Convert.ToBase64String(loadedCertificate.Export(X509ContentType.Cert)));
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

            if (Logger.IsErrorEnabled)
            {
                var errors = GetStdError();
                Logger.Error($"Executing {executable} {args} took {sw.ElapsedMilliseconds:#,#;;0} ms");
                if (!string.IsNullOrWhiteSpace(errors))
                    Logger.Error($"Executing {executable} {args} finished with exit code: {process.ExitCode}. Errors: {errors}");
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

            if (Logger.IsErrorEnabled)
            {
                var errors = GetStdError();
                Logger.Error($"Executing {_config.MasterKeyExec} {_config.MasterKeyExecArguments} took {sw.ElapsedMilliseconds:#,#;;0} ms. Stderr: {errors}");
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

                if (loadedCertificate.SubjectName.Name == null)
                    return;

                var cnValue = loadedCertificate.SubjectName.Name.StartsWith("CN=", StringComparison.OrdinalIgnoreCase)
                    ? loadedCertificate.SubjectName.Name.Substring(3)
                    : loadedCertificate.SubjectName.Name;

                var utcNow = DateTime.UtcNow;
                var existingCerts = userIntermediateStore.Certificates.Find(X509FindType.FindBySubjectName, cnValue, false);
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

        public CertificateUtils.CertificateHolder LoadCertificateFromPath(string path, string password, LicenseType licenseType, bool certificateValidationKeyUsages)
        {
            try
            {
                path = Path.Combine(AppContext.BaseDirectory, path);
                var rawData = File.ReadAllBytes(path);

                // we need to load it as exportable because we might need to send it over the cluster
                var loadedCertificate = CertificateLoaderUtil.CreateCertificate(rawData, password, CertificateLoaderUtil.FlagsForExport);

                ValidateExpiration(path, loadedCertificate, licenseType, throwOnExpired: false);

                ValidatePrivateKey(path, password, rawData, out var privateKey);

                ValidateKeyUsages(path, loadedCertificate, certificateValidationKeyUsages);

                return new CertificateUtils.CertificateHolder(loadedCertificate, privateKey, Convert.ToBase64String(loadedCertificate.Export(X509ContentType.Cert)));
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Could not load certificate file {path}", e);
            }
        }

        internal static void ValidatePrivateKey(string source, string certificatePassword, byte[] rawData, out AsymmetricKeyEntry pk, SetupProgressAndResult progress = null)
        {
            pk = null;
            foreach (string alias in GetAliases(certificatePassword, rawData, out var getKey))
            {
                pk = getKey(alias);
                if (pk != null)
                    break;
            }

            if (pk == null)
            {
                var msg = "Unable to find the private key in the provided certificate from " + source;

                if (Logger.IsErrorEnabled)
                    Logger.Error(msg);
                progress?.AddInfo(msg);

                throw new EncryptionException(msg);
            }

            static IEnumerable GetAliases(string certificatePassword, byte[] rawData, out Func<string, AsymmetricKeyEntry> getKey)
            {
                try
                {
                    var store = new Pkcs12StoreBuilder().Build();
                    store.Load(new MemoryStream(rawData), certificatePassword?.ToCharArray() ?? Array.Empty<char>());

                    getKey = store.GetKey;
                    return store.Aliases;
                }
                catch (Exception)
                {
                    try
                    {
                        // Using a partial copy of the Pkcs12Store class
                        // Workaround for https://github.com/dotnet/corefx/issues/30946

                        var store = new PkcsStoreWorkaroundFor30946();
                        store.Load(new MemoryStream(rawData), certificatePassword?.ToCharArray() ?? Array.Empty<char>());

                        getKey = store.GetKey;
                        return store.Aliases;
                    }
                    catch
                    {
                        // ignore - we prefer the original exception
                    }

                    throw;
                }
            }
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

        private sealed class PkcsStoreWorkaroundFor30946
        {
            // Workaround for https://github.com/dotnet/corefx/issues/30946
            // This class is a partial copy of BouncyCastle's Pkcs12Store which doesn't throw the exception: "attempt to add existing attribute with different value".

            // Explanation: Certificates which were exported in Linux were changed to include a multiple bag attribute (localKeyId).
            // When using BouncyCastle's Pkcs12Store to load the cert, we got an exception: "attempt to add existing attribute with different value",
            // but we don't care about that. We just need to extract the private key when using Load().

            private readonly IgnoresCaseHashtable keys = new IgnoresCaseHashtable();
            private readonly IDictionary localIds = new Hashtable();
            private readonly IgnoresCaseHashtable certs = new IgnoresCaseHashtable();
            private readonly IDictionary chainCerts = new Hashtable();
            private readonly IDictionary keyCerts = new Hashtable();
            private AsymmetricKeyEntry unmarkedKeyEntry = null;

            public void Load(Stream input, char[] password)
            {
                Asn1Sequence obj = (Asn1Sequence)Asn1Object.FromStream(input);
                Pfx bag = Pfx.GetInstance(obj);
                ContentInfo info = bag.AuthSafe;
                bool wrongPkcs12Zero = false;

                if (password != null && bag.MacData != null) // check the mac code
                {
                    MacData mData = bag.MacData;
                    DigestInfo dInfo = mData.Mac;
                    AlgorithmIdentifier algId = dInfo.AlgorithmID;
                    byte[] salt = mData.GetSalt();
                    int itCount = mData.IterationCount.IntValue;

                    byte[] data = ((Asn1OctetString)info.Content).GetOctets();

                    byte[] mac = CalculatePbeMac(algId.Algorithm, salt, itCount, password, false, data);
                    byte[] dig = dInfo.GetDigest();

                    if (!Arrays.FixedTimeEquals(mac, dig))
                    {
                        if (password.Length > 0)
                            throw new IOException("PKCS12 key store MAC invalid - wrong password or corrupted file.");

                        // Try with incorrect zero length password
                        mac = CalculatePbeMac(algId.Algorithm, salt, itCount, password, true, data);

                        if (!Arrays.FixedTimeEquals(mac, dig))
                            throw new IOException("PKCS12 key store MAC invalid - wrong password or corrupted file.");

                        wrongPkcs12Zero = true;
                    }
                }

                keys.Clear();
                localIds.Clear();
                unmarkedKeyEntry = null;

                IList certBags = new ArrayList();

                if (info.ContentType.Equals(PkcsObjectIdentifiers.Data))
                {
                    byte[] octs = ((Asn1OctetString)info.Content).GetOctets();
                    AuthenticatedSafe authSafe = AuthenticatedSafe.GetInstance(
                        (Asn1Sequence)Asn1OctetString.FromByteArray(octs));
                    ContentInfo[] cis = authSafe.GetContentInfo();

                    foreach (ContentInfo ci in cis)
                    {
                        DerObjectIdentifier oid = ci.ContentType;

                        byte[] octets = null;
                        if (oid.Equals(PkcsObjectIdentifiers.Data))
                        {
                            octets = ((Asn1OctetString)ci.Content).GetOctets();
                        }
                        else if (oid.Equals(PkcsObjectIdentifiers.EncryptedData))
                        {
                            if (password != null)
                            {
                                EncryptedData d = EncryptedData.GetInstance(ci.Content);
                                octets = CryptPbeData(false, d.EncryptionAlgorithm,
                                    password, wrongPkcs12Zero, d.Content.GetOctets());
                            }
                        }

                        if (octets != null)
                        {
                            Asn1Sequence seq = (Asn1Sequence)Asn1Object.FromByteArray(octets);

                            foreach (Asn1Sequence subSeq in seq)
                            {
                                SafeBag b = SafeBag.GetInstance(subSeq);

                                if (b.BagID.Equals(PkcsObjectIdentifiers.CertBag))
                                {
                                    certBags.Add(b);
                                }
                                else if (b.BagID.Equals(PkcsObjectIdentifiers.Pkcs8ShroudedKeyBag))
                                {
                                    LoadPkcs8ShroudedKeyBag(EncryptedPrivateKeyInfo.GetInstance(b.BagValue),
                                        b.BagAttributes, password, wrongPkcs12Zero);
                                }
                                else if (b.BagID.Equals(PkcsObjectIdentifiers.KeyBag))
                                {
                                    LoadKeyBag(PrivateKeyInfo.GetInstance(b.BagValue), b.BagAttributes);
                                }
                                else
                                {
                                    // TODO Other bag types
                                }
                            }
                        }
                    }
                }

                foreach (SafeBag b in certBags)
                {
                    CertBag certBag = CertBag.GetInstance((Asn1Sequence)b.BagValue);
                    byte[] octets = ((Asn1OctetString)certBag.CertValue).GetOctets();
                    Org.BouncyCastle.X509.X509Certificate cert = new X509CertificateParser().ReadCertificate(octets);

                    //
                    // set the attributes
                    //
                    IDictionary<DerObjectIdentifier, Asn1Encodable> attributes = new Dictionary<DerObjectIdentifier, Asn1Encodable>();
                    Asn1OctetString localId = null;
                    string alias = null;

                    if (b.BagAttributes != null)
                    {
                        foreach (Asn1Sequence sq in b.BagAttributes)
                        {
                            DerObjectIdentifier aOid = DerObjectIdentifier.GetInstance(sq[0]);
                            Asn1Set attrSet = Asn1Set.GetInstance(sq[1]);

                            if (attrSet.Count > 0)
                            {
                                // TODO We should be adding all attributes in the set
                                Asn1Encodable attr = attrSet[0];

                                // TODO We might want to "merge" attribute sets with
                                // the same OID - currently, differing values give an error
                                if (attributes.ContainsKey(aOid))
                                {
                                    // OK, but the value has to be the same
                                    if (!attributes[aOid].Equals(attr))
                                    {
                                        //throw new IOException("attempt to add existing attribute with different value");
                                    }
                                }
                                else
                                {
                                    attributes.Add(aOid, attr);
                                }

                                if (aOid.Equals(PkcsObjectIdentifiers.Pkcs9AtFriendlyName))
                                {
                                    alias = ((DerBmpString)attr).GetString();
                                }
                                else if (aOid.Equals(PkcsObjectIdentifiers.Pkcs9AtLocalKeyID))
                                {
                                    localId = (Asn1OctetString)attr;
                                }
                            }
                        }
                    }

                    CertId certId = new CertId(cert.GetPublicKey());
                    X509CertificateEntry certEntry = new X509CertificateEntry(cert, attributes);

                    chainCerts[certId] = certEntry;

                    if (unmarkedKeyEntry != null)
                    {
                        if (keyCerts.Count == 0)
                        {
                            string name = Hex.ToHexString(certId.Id);

                            keyCerts[name] = certEntry;
                            keys[name] = unmarkedKeyEntry;
                        }
                    }
                    else
                    {
                        if (localId != null)
                        {
                            string name = Hex.ToHexString(localId.GetOctets());

                            keyCerts[name] = certEntry;
                        }

                        if (alias != null)
                        {
                            // TODO There may have been more than one alias
                            certs[alias] = certEntry;
                        }
                    }
                }
            }

            public AsymmetricKeyEntry GetKey(
                string alias)
            {
                if (alias == null)
                    throw new ArgumentNullException("alias");

                return (AsymmetricKeyEntry)keys[alias];
            }

            public IEnumerable Aliases
            {
                get { return GetAliasesTable().Keys; }
            }

            private IDictionary<string, string> GetAliasesTable()
            {
                IDictionary<string, string> tab = new Dictionary<string, string>();

                foreach (string key in certs.Keys)
                {
                    tab[key] = "cert";
                }

                foreach (string a in keys.Keys)
                {
                    if (tab[a] == null)
                    {
                        tab[a] = "key";
                    }
                }

                return tab;
            }

            internal static byte[] CalculatePbeMac(
                DerObjectIdentifier oid,
                byte[] salt,
                int itCount,
                char[] password,
                bool wrongPkcs12Zero,
                byte[] data)
            {
                Asn1Encodable asn1Params = PbeUtilities.GenerateAlgorithmParameters(
                    oid, salt, itCount);
                ICipherParameters cipherParams = PbeUtilities.GenerateCipherParameters(
                    oid, password, wrongPkcs12Zero, asn1Params);

                IMac mac = (IMac)PbeUtilities.CreateEngine(oid);
                mac.Init(cipherParams);
                return MacUtilities.DoFinal(mac, data);
            }

            private byte[] CryptPbeData(
                bool forEncryption,
                AlgorithmIdentifier algId,
                char[] password,
                bool wrongPkcs12Zero,
                byte[] data)
            {
                IBufferedCipher cipher = PbeUtilities.CreateEngine(algId.Algorithm) as IBufferedCipher;

                if (cipher == null)
                    throw new Exception("Unknown encryption algorithm: " + algId.Algorithm);

                Pkcs12PbeParams pbeParameters = Pkcs12PbeParams.GetInstance(algId.Parameters);
                ICipherParameters cipherParams = PbeUtilities.GenerateCipherParameters(
                    algId.Algorithm, password, wrongPkcs12Zero, pbeParameters);
                cipher.Init(forEncryption, cipherParams);
                return cipher.DoFinal(data);
            }

            private static SubjectKeyIdentifier CreateSubjectKeyID(
                AsymmetricKeyParameter pubKey)
            {
                return new SubjectKeyIdentifier(
                    SubjectPublicKeyInfoFactory.CreateSubjectPublicKeyInfo(pubKey));
            }

            private sealed class IgnoresCaseHashtable
                : IEnumerable
            {
                private readonly IDictionary orig = new Hashtable();
                private readonly IDictionary keys = new Hashtable();

                public void Clear()
                {
                    orig.Clear();
                    keys.Clear();
                }

                public IEnumerator GetEnumerator()
                {
                    return orig.GetEnumerator();
                }

                public ICollection Keys
                {
                    get { return orig.Keys; }
                }

                public object this[
                    string alias]
                {
                    get
                    {
                        string upper = alias.ToUpper(CultureInfo.InvariantCulture);
                        string k = (string)keys[upper];

                        if (k == null)
                            return null;

                        return orig[k];
                    }
                    set
                    {
                        string upper = alias.ToUpper(CultureInfo.InvariantCulture);
                        string k = (string)keys[upper];
                        if (k != null)
                        {
                            orig.Remove(k);
                        }

                        keys[upper] = alias;
                        orig[alias] = value;
                    }
                }
            }

            private void LoadPkcs8ShroudedKeyBag(EncryptedPrivateKeyInfo encPrivKeyInfo, Asn1Set bagAttributes,
                char[] password, bool wrongPkcs12Zero)
            {
                if (password != null)
                {
                    PrivateKeyInfo privInfo = PrivateKeyInfoFactory.CreatePrivateKeyInfo(
                        password, wrongPkcs12Zero, encPrivKeyInfo);

                    LoadKeyBag(privInfo, bagAttributes);
                }
            }

            private void LoadKeyBag(PrivateKeyInfo privKeyInfo, Asn1Set bagAttributes)
            {
                AsymmetricKeyParameter privKey = PrivateKeyFactory.CreateKey(privKeyInfo);

                Dictionary<DerObjectIdentifier, Asn1Encodable> attributes = new();
                AsymmetricKeyEntry keyEntry = new AsymmetricKeyEntry(privKey, attributes);

                string alias = null;
                Asn1OctetString localId = null;

                if (bagAttributes != null)
                {
                    foreach (Asn1Sequence sq in bagAttributes)
                    {
                        DerObjectIdentifier aOid = DerObjectIdentifier.GetInstance(sq[0]);
                        Asn1Set attrSet = Asn1Set.GetInstance(sq[1]);
                        Asn1Encodable attr = null;

                        if (attrSet.Count > 0)
                        {
                            // TODO We should be adding all attributes in the set
                            attr = attrSet[0];

                            // TODO We might want to "merge" attribute sets with
                            // the same OID - currently, differing values give an error
                            if (attributes.ContainsKey(aOid))
                            {
                                // OK, but the value has to be the same
                                if (!attributes[aOid].Equals(attr))
                                    throw new IOException("attempt to add existing attribute with different value");
                            }
                            else
                            {
                                attributes.Add(aOid, attr);
                            }

                            if (aOid.Equals(PkcsObjectIdentifiers.Pkcs9AtFriendlyName))
                            {
                                alias = ((DerBmpString)attr).GetString();
                                // TODO Do these in a separate loop, just collect aliases here
                                keys[alias] = keyEntry;
                            }
                            else if (aOid.Equals(PkcsObjectIdentifiers.Pkcs9AtLocalKeyID))
                            {
                                localId = (Asn1OctetString)attr;
                            }
                        }
                    }
                }

                if (localId != null)
                {
                    string name = Hex.ToHexString(localId.GetOctets());

                    if (alias == null)
                    {
                        keys[name] = keyEntry;
                    }
                    else
                    {
                        // TODO There may have been more than one alias
                        localIds[alias] = name;
                    }
                }
                else
                {
                    unmarkedKeyEntry = keyEntry;
                }
            }

            private sealed class CertId
            {
                private readonly byte[] _id;

                internal CertId(AsymmetricKeyParameter pubKey)
                {
                    _id = CreateSubjectKeyID(pubKey).GetKeyIdentifier();
                }

                internal byte[] Id
                {
                    get { return _id; }
                }

                public override int GetHashCode()
                {
#pragma warning disable RS1024 // Compare symbols correctly
                    return Arrays.GetHashCode(_id);
#pragma warning restore RS1024 // Compare symbols correctly
                }

                public override bool Equals(object obj)
                {
                    if (obj == this)
                        return true;

                    CertId other = obj as CertId;

                    if (other == null)
                        return false;

                    return Arrays.AreEqual(_id, other._id);
                }
            }
        }

    }
}

