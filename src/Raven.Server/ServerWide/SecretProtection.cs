using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;
using Raven.Server.Config.Categories;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Voron.Platform.Posix;
using OpenFlags = Voron.Platform.Posix.OpenFlags;

namespace Raven.Server.ServerWide
{
    public unsafe class SecretProtection
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RavenServer>("Raven/Secrets");
        private readonly Lazy<byte[]> _serverMasterKey;
        private readonly SecurityConfiguration _config;
        private const int KeySize = 512; // sector size

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
                    debug = _config.CertificateExec + " " + _config.CertificateExecArguments;
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
                var buffer = new byte[KeySize];
                fixed (byte* pBuf = buffer)
                {
                    if (Directory.Exists(dirpath) == false)
                        Directory.CreateDirectory(dirpath);

                    var fd = Syscall.open(filepath, PerPlatformValues.OpenFlags.O_CREAT | OpenFlags.O_RDWR,
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

                        if (size == KeySize)
                        {
                            byte* pos = pBuf;
                            long amountRead = 0;
                            while (amountRead < KeySize)
                            {
                                var read = Syscall.pread(fd, pos, (ulong)(KeySize - amountRead), amountRead);
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
                            if (amountRead != KeySize)
                                throw new FileLoadException($"Failed to read the full key size from {filepath}, expected to read {KeySize} but go only {amountRead}");
                        }
                        else // we assume that if the size isn't a key size, then it was never valid and regenerate the key
                        {
                            Sodium.randombytes_buf(pBuf, (UIntPtr)KeySize);

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

                            var len = KeySize;
                            while (len > 0)
                            {
                                var writeAmount = Syscall.write(fd, pBuf, KeySize);
                                if (writeAmount <= 0) // 0 will be considered as error here
                                {
                                    var err = Marshal.GetLastWin32Error();
                                    Syscall.ThrowLastError(err, $"Failed to write {KeySize} bytes into {filepath}, only wrote {len}");
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

        public byte[] Protect(byte[] secret, byte[] entropy)
        {
            if (PlatformDetails.RunningOnPosix == false && _config.MasterKeyExec == null && _config.MasterKeyPath == null)
                return ProtectedData.Protect(secret, entropy, DataProtectionScope.CurrentUser);

            var protectedData = new byte[secret.Length + Sodium.crypto_aead_chacha20poly1305_ABYTES()];
            var key = _serverMasterKey.Value;

            if (entropy.Length < 8)
                throw new InvalidOperationException($"The provided entropy is too small. Should be at least 8 bytes but was {entropy.Length} bytes");

            fixed (byte* pSecret = secret)
            fixed (byte* pProtectedData = protectedData)
            fixed (byte* pEntropy = entropy)
            fixed (byte* pKey = key)
            {
                ulong cLen;
                var rc = Sodium.crypto_aead_chacha20poly1305_encrypt(
                    pProtectedData,
                    &cLen,
                    pSecret,
                    (ulong)secret.Length,
                    null,
                    0,
                    null,
                    pEntropy,
                    pKey
                );

                Debug.Assert(cLen <= (ulong)secret.Length + (ulong)Sodium.crypto_aead_chacha20poly1305_ABYTES());

                if (rc != 0)
                    throw new InvalidOperationException($"Unable to protect secret, rc={rc}");
            }
            return protectedData;
        }

        public byte[] Unprotect(byte[] secret, byte[] entropy)
        {
            if (PlatformDetails.RunningOnPosix == false && _config.MasterKeyExec == null && _config.MasterKeyPath == null)
                return ProtectedData.Unprotect(secret, entropy, DataProtectionScope.CurrentUser);

            var unprotectedData = new byte[secret.Length - Sodium.crypto_aead_chacha20poly1305_ABYTES()];
            var key = _serverMasterKey.Value;

            if (entropy.Length < 8)
                throw new InvalidOperationException($"The provided entropy is too small. Should be at least 8 bytes but was {entropy.Length} bytes");

            fixed (byte* pSecret = secret)
            fixed (byte* pUnprotectedData = unprotectedData)
            fixed (byte* pEntropy = entropy)
            fixed (byte* pKey = key)
            {
                ulong mLen;
                var rc = Sodium.crypto_aead_chacha20poly1305_decrypt(
                    pUnprotectedData,
                    &mLen,
                    null,
                    pSecret,
                    (ulong)secret.Length,
                    null,
                    0,
                    pEntropy,
                    pKey
                );

                Debug.Assert(mLen <= (ulong)secret.Length - (ulong)Sodium.crypto_aead_chacha20poly1305_ABYTES());

                if (rc != 0)
                    throw new InvalidOperationException($"Unable to unprotect secret, rc={rc}");
            }
            return unprotectedData;
        }

        public RavenServer.CertificateHolder LoadCertificateWithExecutable(string executable, string args)
        {
            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = executable,
                    Arguments = args,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                }
            };

            var sw = Stopwatch.StartNew();

            try
            {
                process.Start();
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
                throw new InvalidOperationException(
                    $"Unable to get certificate by executing {executable} {args}, waited for {_config.CertificateExecTimeout} ms but the process didn't exit. Stderr: {GetStdError()}",
                    e);

            }

            if (Logger.IsOperationsEnabled)
            {
                var errors = GetStdError();
                Logger.Operations(string.Format($"Executing {executable} {args} took {sw.ElapsedMilliseconds:#,#;;0} ms"));
                if (!string.IsNullOrWhiteSpace(errors))
                    Logger.Operations(string.Format($"Executing {executable} {args} finished with exit code: {process.ExitCode}. Errors: {errors}"));
            }

            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Unable to get certificate by executing {executable} {args}, the exit code was {process.ExitCode}. Stderr: {GetStdError()}");
            }

            var rawData = ms.ToArray();
            X509Certificate2 loadedCertificate;
            AsymmetricKeyEntry privateKey;
            try
            {
                loadedCertificate = new X509Certificate2(rawData);
                ValidateExpiration(executable, loadedCertificate);
                ValidatePrivateKey(executable, null, rawData, out  privateKey);
                ValidateKeyUsages(executable, loadedCertificate);

            }
            catch (Exception e)
            {         
                throw new InvalidOperationException($"Got invalid certificate via {executable} {args}", e);
            }
        
            return new RavenServer.CertificateHolder
            {
                Certificate = loadedCertificate,
                CertificateForClients = Convert.ToBase64String(loadedCertificate.Export(X509ContentType.Cert)),
                PrivateKey = privateKey
            };
        }

        private byte[] LoadMasterKeyWithExecutable()
        {
            const int keySize = 512;

            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = _config.MasterKeyExec,
                    Arguments = _config.MasterKeyExecArguments,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                }
            };

            var sw = Stopwatch.StartNew();

            try
            {
                process.Start();
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
                Logger.Operations(string.Format($"Executing {_config.MasterKeyExec} {_config.MasterKeyExecArguments} took {sw.ElapsedMilliseconds:#,#;;0} ms. Stderr: {errors}"));
            }

            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Unable to get master key by executing {_config.MasterKeyExec} {_config.MasterKeyExecArguments}, the exit code was {process.ExitCode}. Stderr: {GetStdError()}");
            }

            var rawData = ms.ToArray();

            if (rawData.Length * 8 != keySize)
            {
                throw new InvalidOperationException(
                    $"Got wrong master key after executing {_config.MasterKeyExec} {_config.MasterKeyExecArguments}, the size of the key must be {keySize} bits, but was {rawData.Length * 8} bits.");
            }

            return rawData;
        }

        public X509Certificate2 LoadProxyCertificateFromPath(string path, string password)
        {
            var rawData = File.ReadAllBytes(path);

            var loadedCertificate = password == null
                ? new X509Certificate2(rawData)
                : new X509Certificate2(rawData, password);

            return loadedCertificate;
        }

        public RavenServer.CertificateHolder LoadCertificateFromBase64(string certificate, string password)
        {
            var source = "settings.json";
            try
            {
                byte[] certBytes;
                try
                {
                    certBytes = Convert.FromBase64String(certificate);
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Unable to parse the {certificate} property, expected a Base64 value", e);
                }

                var loadedCertificate = new X509Certificate2(certBytes, password);

                return ValidateCertificateAndCreateCertificateHolder(certificate, source, loadedCertificate, certBytes);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Could not load certificate from {source}", e);
            }
        }

        public static RavenServer.CertificateHolder ValidateCertificateAndCreateCertificateHolder(string certificate, string source, X509Certificate2 loadedCertificate, byte[] certBytes)
        {
            ValidateExpiration(source, loadedCertificate);

            ValidatePrivateKey(source, null, certBytes, out var privateKey);

            ValidateKeyUsages(source, loadedCertificate);

            return new RavenServer.CertificateHolder
            {
                Certificate = loadedCertificate,
                CertificateForClients = certificate,
                PrivateKey = privateKey
            };
        }

        public RavenServer.CertificateHolder LoadCertificateFromPath(string path, string password)
        {
            try
            {
                var rawData = File.ReadAllBytes(path);

                var loadedCertificate = password == null
                    ? new X509Certificate2(rawData)
                    : new X509Certificate2(rawData, password);

                ValidateExpiration(path, loadedCertificate);

                ValidatePrivateKey(path, password, rawData, out var privateKey);

                ValidateKeyUsages(path, loadedCertificate);

                return new RavenServer.CertificateHolder
                {
                    Certificate = loadedCertificate,
                    CertificateForClients = Convert.ToBase64String(loadedCertificate.Export(X509ContentType.Cert)),
                    PrivateKey = privateKey
                };
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Could not load certificate file {path}", e);
            }
        }

        public byte[] LoadMasterKeyFromPath()
        {
            try
            {
                var key = File.ReadAllBytes(_config.MasterKeyPath);

                if (key.Length * 8 != KeySize)
                {
                    throw new InvalidOperationException(
                        $"The size of the key must be {KeySize} bits, but was {key.Length * 8} bits.");
                }
                return key;
            }
            catch (Exception e)
            {
                throw new CryptographicException(
                    $"Unable to open the master secret key at {_config.MasterKeyPath}, won't proceed because losing this key will lose access to all user encrypted information. Admin assistance required.",
                    e);
            }
        }

        private static void ValidateExpiration(string source, X509Certificate2 loadedCertificate)
        {
            if (loadedCertificate.NotAfter < DateTime.UtcNow)
                throw new EncryptionException($"The provided certificate {loadedCertificate.FriendlyName} from {source} is expired! " + loadedCertificate);
        }

        private static void ValidatePrivateKey(string source, string certificatePassword, byte[] rawData, out AsymmetricKeyEntry pk)
        {
            var store = new Pkcs12Store();
            store.Load(new MemoryStream(rawData), certificatePassword?.ToCharArray() ?? Array.Empty<char>());
            pk = null;
            foreach (string alias in store.Aliases)
            {
                pk = store.GetKey(alias);
                if (pk != null)
                    break;
            }

            if (pk == null)
                throw new EncryptionException("Unable to find the private key in the provided certificate from " + source);
        }

        private static void ValidateKeyUsages(string source, X509Certificate2 loadedCertificate)
        {
            var supported = false;
            foreach (var extension in loadedCertificate.Extensions)
            {
                if (extension.Oid.Value != "2.5.29.37") //Enhanced Key Usage extension
                    continue;

                var extensionString = new AsnEncodedData(extension.Oid, extension.RawData).Format(false);

                // There is a difference in the ASN string between Linux and Windows, see RavenDB-8489
                // TODO: see if there is a better way to extract the oids, and not rely on the ASN string
                supported = (extensionString.Contains("Client Authentication") && extensionString.Contains("Server Authentication"))
                            || (extensionString.Contains("1.3.6.1.5.5.7.3.2") && extensionString.Contains("1.3.6.1.5.5.7.3.1"));
            }

            if (supported == false)
                throw new EncryptionException("Server certificate " + loadedCertificate.FriendlyName + "from " + source +
                                              " must be defined with the following 'Enhanced Key Usages': Client Authentication (Oid 1.3.6.1.5.5.7.3.2) & Server Authentication (Oid 1.3.6.1.5.5.7.3.1)");
        }
    }
}
