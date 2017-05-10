using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Collation.Cultures;
using Sparrow;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Voron.Platform.Posix;

namespace Raven.Server.ServerWide
{
    public static unsafe class SecretProtection
    {
        private static readonly Lazy<byte[]> PosixMasterKey = new Lazy<byte[]>(() =>
        {
            var dirpath = Path.Combine(Environment.GetEnvironmentVariable("HOME"), ".ravendb");
            dirpath = Path.GetFullPath(dirpath);
            var filepath = Path.Combine(dirpath, "secret.key");
            const int keySize = 512; // sector size
            var buffer = new byte[keySize];
            fixed (byte* pBuf = buffer)
            {
                try
                {
                    if (Directory.Exists(dirpath) == false)
                        Directory.CreateDirectory(dirpath);

                    var fd = Syscall.open(filepath, OpenFlags.O_CREAT | OpenFlags.O_RDWR,
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

                        if (size == keySize)
                        {
                            byte* pos = pBuf;
                            long amountRead = 0;
                            while (amountRead < keySize)
                            {
                                var read = Syscall.pread(fd, pos, (ulong)(keySize - amountRead), amountRead);
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
                            if (amountRead != keySize)
                                throw new FileLoadException($"Failed to read the full key size from {filepath}, expected to read {keySize} but go only {amountRead}");
                        }
                        else // we assume that if the size isn't a key size, then it was never valid and regenerate the key
                        {
                            Sodium.randombytes_buf(pBuf, keySize);
                          
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

                            var writeAmount = Syscall.write(fd, pBuf, keySize);
                            if (writeAmount != keySize)
                            {
                                var err = Marshal.GetLastWin32Error();
                                Syscall.ThrowLastError(err, $"Failed to write {buffer.Length} bytes into {filepath}, only wrote {writeAmount}");
                            }

                            if (Syscall.fsync(fd) != 0)
                            {
                                var err = Marshal.GetLastWin32Error();
                                Syscall.ThrowLastError(err, $"Failed to fsync {filepath}");
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
                    throw new CryptographicException(
                        $"Unable to open the master secret key at {filepath}, won't proceed because losing this key will lose access to all user encrypted information. Admin assistance required.",
                        e);
                }
            }

        });

        public static byte[] Protect(byte[] secret, byte[] entropy)
        {
            if (PlatformDetails.RunningOnPosix == false)
                return ProtectedData.Protect(secret, entropy, DataProtectionScope.CurrentUser);

            var protectedData = new byte[secret.Length + Sodium.crypto_aead_chacha20poly1305_ABYTES()];
            var key = PosixMasterKey.Value;

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

        public static byte[] Unprotect(byte[] secret, byte[] entropy)
        {
            if (PlatformDetails.RunningOnPosix == false)
                return ProtectedData.Unprotect(secret, entropy, DataProtectionScope.CurrentUser);
            var unprotectedData = new byte[secret.Length - Sodium.crypto_aead_chacha20poly1305_ABYTES()];
            var key = PosixMasterKey.Value;

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
    }
}
