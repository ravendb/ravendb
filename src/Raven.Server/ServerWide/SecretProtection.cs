using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;
using Raven.Client;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Collation.Cultures;
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
            if (_config.MasterKeyExec != null)
            {
                return LoadMasterKeyWithExecutable();
            }

            if (_config.MasterKeyPath != null)
            {
                return LoadMasterKeyFromPath();
            }

            // POSIX only
            var dirpath = Path.Combine(Environment.GetEnvironmentVariable("HOME"), ".ravendb");
            dirpath = Path.GetFullPath(dirpath);
            var filepath = Path.Combine(dirpath, "secret.key");
            
            var buffer = new byte[KeySize];
            fixed (byte* pBuf = buffer)
            {
                try
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

                            var writeAmount = Syscall.write(fd, pBuf, KeySize);
                            if (writeAmount != KeySize)
                            {
                                var err = Marshal.GetLastWin32Error();
                                Syscall.ThrowLastError(err, $"Failed to write {buffer.Length} bytes into {filepath}, only wrote {writeAmount}");
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
                catch (Exception e)
                {
                    throw new CryptographicException($"Unable to open the master secret key at {filepath}, won't proceed because losing this key will lose access to all user encrypted information. Admin assistance required.", e);
                }
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

        public RavenServer.CertificateHolder LoadCerificateWithExecutable()
        {
            var path = _config.CertificateExec;
            var args = _config.CertificateExecArguments;
            var timeout = _config.CertificateExecTimout;

            if (string.IsNullOrEmpty(path))
                return null;

            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = path,
                    Arguments = args,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = false
                }
            };

            var sw = Stopwatch.StartNew();

            try
            {
                process.Start();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to get certificate by executing {path} {args}. Failed to start process.", e);
            }

            var ms = new MemoryStream();
            var readErrors = process.StandardError.ReadToEndAsync();
            var readStdOut = process.StandardOutput.BaseStream.CopyToAsync(ms);

            if (process.WaitForExit(_config.CertificateExecTimout) == false)
            {
                throw new InvalidOperationException($"Unable to get certificate by executing {path} {args}, waited for {timeout} ms but the process didn't exit.");
            }
            readStdOut.Wait(_config.CertificateExecTimout);
            readErrors.Wait(_config.CertificateExecTimout);

            if (Logger.IsOperationsEnabled)
            {
                Logger.Operations(string.Format($"Executing {path} {args} took {sw.ElapsedMilliseconds:#,#;;0} ms"));
                if (!string.IsNullOrWhiteSpace(readErrors.Result))
                    Logger.Operations(string.Format($"Executing {path} {args} finished with exit code: {process.ExitCode}. Errors: {readErrors.Result}"));
            }

            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Unable to get certificate by executing {path} {args}, the exit code was {process.ExitCode} and the error was {readErrors.Result}");
            }

            var rawData = ms.ToArray();
            var loadedCertificate = new X509Certificate2(rawData);
            if (loadedCertificate.HasPrivateKey == false)
            {
                throw new InvalidOperationException(
                    $"Certificate error after executing {path} {args}, the provided certificate doesn't have a private key.");
            }

            ValidateExpiration(path, loadedCertificate);
            ValidatePrivateKey(path, null, rawData, out var privateKey);
            ValidateKeyUsages(path, loadedCertificate);

            return new RavenServer.CertificateHolder
            {
                Certificate = loadedCertificate,
                CertificateForClients = Convert.ToBase64String(loadedCertificate.Export(X509ContentType.Cert)),
                PrivateKey = privateKey
            };
        }

        public byte[] LoadMasterKeyWithExecutable()
        {
            var path = _config.MasterKeyExec;
            var args = _config.MasterKeyExecArguments;
            var timeout = _config.MasterKeyExecTimout;

            const int keySize = 512;
            
            if (string.IsNullOrEmpty(path))
                return null;

            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = path,
                    Arguments = args,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = false
                }
            };

            var sw = Stopwatch.StartNew();

            try
            {
                process.Start();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to get master key by executing {path} {args}. Failed to start process.", e);
            }

            var ms = new MemoryStream();
            var readErrors = process.StandardError.ReadToEndAsync();
            var readStdOut = process.StandardOutput.BaseStream.CopyToAsync(ms);

            if (process.WaitForExit(timeout) == false)
            {
                throw new InvalidOperationException($"Unable to get master key by executing {path} {args}, waited for {timeout} ms but the process didn't exit.");
            }
            readStdOut.Wait(timeout);
            readErrors.Wait(timeout);

            if (Logger.IsOperationsEnabled)
            {
                Logger.Operations(string.Format($"Executing {path} {args} took {sw.ElapsedMilliseconds:#,#;;0} ms"));
                if (!string.IsNullOrWhiteSpace(readErrors.Result))
                    Logger.Operations(string.Format($"Executing {path} {args} finished with exit code: {process.ExitCode}. Errors: {readErrors.Result}"));
            }

            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Unable to get master key by executing {path} {args}, the exit code was {process.ExitCode} and the error was {readErrors.Result}");
            }

            var rawData = ms.ToArray();

            if (rawData.Length * 8 != keySize)
            {
                throw new InvalidOperationException(
                    $"Got wrong master key after executing {path} {args}, the size of the key must be {keySize} bits, but was {rawData.Length * 8} bits.");
            }

            return rawData;
        }

        public RavenServer.CertificateHolder LoadCertificateFromPath(string certificatePath = null, string certificatePassword = null)
        {
            var path = certificatePath ?? _config.CertificatePath;
            var password = certificatePassword ?? _config.CertificatePassword;
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
                throw new InvalidOperationException($"Could not load certificate file {path}, please check the path and password", e);
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

                supported = extensionString.Contains("1.3.6.1.5.5.7.3.2") && extensionString.Contains("1.3.6.1.5.5.7.3.1"); // Client Authentication & Server Authentication
            }

            if (supported == false)
                throw new EncryptionException("Server certificate " + loadedCertificate.FriendlyName + "from " + source + " must be defined with the following 'Enhanced Key Usages': Client Authentication (Oid 1.3.6.1.5.5.7.3.2) & Server Authentication (Oid 1.3.6.1.5.5.7.3.1)");
        }
    }


}
