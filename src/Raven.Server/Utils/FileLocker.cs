using System;
using System.Collections.Concurrent;
using System.IO;
using Sparrow.Logging;

namespace Raven.Server.Utils
{
    public struct FileLocker : IDisposable
    {
        private static readonly ConcurrentDictionary<string, object> _inMemoryLocks = new ConcurrentDictionary<string, object>();

        private bool _inMemLock;
        private readonly string _lockFile;
        private FileStream _writeLockFile;

        public FileLocker(string lockFile)
        {
            _lockFile = lockFile;
            _inMemLock = false;
            _writeLockFile = null;
        }

        public void TryAcquireWriteLock(Logger logger)
        {
            var dir = Path.GetDirectoryName(_lockFile);
            try
            {
                if (Directory.Exists(dir) == false)
                    Directory.CreateDirectory(dir);

                _writeLockFile = new FileStream(_lockFile, FileMode.Create,
                    FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.DeleteOnClose);
                _writeLockFile.SetLength(1);
                _writeLockFile.Lock(0, 1);
            }
            catch (PlatformNotSupportedException)
            {
                // locking part of the file isn't supported on macOS
                // new FileStream will lock the file on this platform
            }
            catch (Exception e)
            {
                _writeLockFile?.Dispose();
                _writeLockFile = null;
                string additionalInfo = null;
                try
                {
                    var drive = new DriveInfo(dir);
                    additionalInfo = "File system type: " + drive.DriveFormat;
                    switch (drive.DriveFormat)
                    {
                        case "v9fs":
                            {
                                // this is a scenario where we are running on Windows, but
                                // we are inside Docker running in Linux with a shared volume.
                                // The v9fs doesn't support file locking, and this isn't likely to be
                                // a production scenario, only for development, so we'll allow it.
                                if (logger.IsInfoEnabled)
                                {
                                    logger.Info($"Unable to take file lock on {_lockFile} with path mounted on {drive.DriveFormat}. " +
                                                "This is likely a Docker instance running Linux from a Windows host, a developer only scenario. " +
                                                "We implement in process \"file locking\" instead.");
                                }

                                if (_inMemoryLocks.TryAdd(_lockFile, null) == false)
                                    throw new InvalidOperationException("Cannot open database because RavenDB was unable to create in memory file lock on: " + _lockFile);

                                _inMemLock = true;
                                return;
                            }
                    }
                }
                catch (Exception a)
                {
                    // couldn't check, just ignore and raise the original error
                    if (logger.IsInfoEnabled)
                    {
                        logger.Info($"Unable to query the drive type after failing to lock file: " + _lockFile, a);
                    }
                }
                throw new InvalidOperationException($"Cannot open database because RavenDB was unable create file lock on: '{_lockFile}'. {additionalInfo}", e);
            }
        }

        public void Dispose()
        {
            if (_writeLockFile != null)
            {
                try
                {
                    _writeLockFile.Unlock(0, 1);
                }
                catch (PlatformNotSupportedException)
                {
                    // Unlock isn't supported on macOS
                }
                _writeLockFile.Dispose();
                try
                {
                    if (File.Exists(_lockFile))
                        File.Delete(_lockFile);
                }
                catch (IOException)
                {
                }
            }
            else if (_inMemLock)
            {
                _inMemoryLocks.TryRemove(_lockFile, out _);
                _inMemLock = false;
            }
        }
    }
}
