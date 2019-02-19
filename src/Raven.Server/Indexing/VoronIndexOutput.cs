using System;
using System.IO;
using Lucene.Net.Store;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Exceptions;
using Sparrow.Server.Platform.Posix;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using Voron.Impl;
using Voron;
using Voron.Platform.Win32;

namespace Raven.Server.Indexing
{
    public class VoronIndexOutput : BufferedIndexOutput
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LuceneVoronDirectory>("VoronIndexOutput");

        private readonly string _name;
        private readonly string _tree;
        private readonly Transaction _tx;
        private readonly Stream _file;
        private readonly string _fileTempPath;
        private readonly IndexOutputFilesSummary _indexOutputFilesSummary;

        public VoronIndexOutput(
            StorageEnvironmentOptions options,
            string name,
            Transaction tx,
            string tree,
            IndexOutputFilesSummary indexOutputFilesSummary)
        {
            _name = name;
            _tree = tree;
            _tx = tx;
            _fileTempPath = options.TempPath.Combine(name + "_" + Guid.NewGuid()).FullPath;
            _indexOutputFilesSummary = indexOutputFilesSummary;

            _file = InitFileStream(options);

            _tx.ReadTree(_tree).AddStream(name, Stream.Null); // ensure it's visible by LuceneVoronDirectory.FileExists, the actual write is inside Dispose
        }

        private Stream InitFileStream(StorageEnvironmentOptions options)
        {
            try
            {
                if (options.EncryptionEnabled)
                    return new TempCryptoStream(_fileTempPath);

                return SafeFileStream.Create(_fileTempPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.DeleteOnClose);
            }
            catch (IOException ioe) when (IsOutOfDiskSpaceException(ioe))
            {
                ThrowDiskFullException();

                // never reached
                return null;
            }
        }

        public override void FlushBuffer(byte[] b, int offset, int len)
        {
            try
            {
                _file.Write(b, offset, len);
                _indexOutputFilesSummary.Increment(len);
            }
            catch (IOException ioe) when (IsOutOfDiskSpaceException(ioe))
            {
                ThrowDiskFullException();
            }
        }

        /// <summary>Random-access methods </summary>
        public override void Seek(long pos)
        {
            base.Seek(pos);
            _file.Seek(pos, SeekOrigin.Begin);
        }

        public override long Length => _file.Length;

        public override void SetLength(long length)
        {
            _file.SetLength(length);
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                base.Dispose(disposing);

                CopyFileStream();
            }
            finally
            {
                DisposeFile();
            }
        }

        private void CopyFileStream()
        {
            if (_indexOutputFilesSummary.HasVoronWriteErrors)
            {
                // we cannot modify the tx anymore 
                return;
            }

            try
            {
                var files = _tx.ReadTree(_tree);

                using (Slice.From(_tx.Allocator, _name, out var nameSlice))
                {
                    _file.Seek(0, SeekOrigin.Begin);
                    files.AddStream(nameSlice, _file);
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to copy the file: {_name}", e);

                _indexOutputFilesSummary.SetWriteError();

                if (e is IOException ioe && IsOutOfDiskSpaceException(ioe))
                {
                    // can happen when trying to copy from the file stream
                    ThrowDiskFullException();
                }
                
                throw;
            }
        }

        private void DisposeFile()
        {
            try
            {
                _file.Dispose();
            }
            catch (Exception e)
            {
                // we are done with this file, nothing we can do here
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to dispose the file: {_name} in path: {_fileTempPath}", e);
            }
            finally
            {
                PosixFile.DeleteOnClose(_fileTempPath);
            }
        }

        private void ThrowDiskFullException()
        {
            var directory = Path.GetDirectoryName(_fileTempPath);
            var driveInfo = DiskSpaceChecker.GetDiskSpaceInfo(directory);
            var freeSpace = driveInfo != null ? driveInfo.TotalFreeSpace.ToString() : "N/A";
            throw new DiskFullException($"There isn't enough space to flush the buffer of the file: {_fileTempPath}. " +
                                        $"Currently available space: {freeSpace}");
        }

        private static bool IsOutOfDiskSpaceException(IOException ioe)
        {
            var expectedDiskFullError = PlatformDetails.RunningOnPosix ? (int)Errno.ENOSPC : (int)Win32NativeFileErrors.ERROR_DISK_FULL;
            var errorCode = PlatformDetails.RunningOnPosix ? ioe.HResult : ioe.HResult & 0xFFFF;
            return errorCode == expectedDiskFullError;
        }
    }
}
