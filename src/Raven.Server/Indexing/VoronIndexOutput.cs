using System;
using System.IO;
using System.Threading;
using Lucene.Net.Store;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Server.Exceptions;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using Voron.Impl;
using Voron;

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
            _fileTempPath = GetTempFilePath(options, name);
            _indexOutputFilesSummary = indexOutputFilesSummary;

            _file = InitFileStream(options);

            _tx.ReadTree(_tree).AddStream(name, Stream.Null); // ensure it's visible by LuceneVoronDirectory.FileExists, the actual write is inside Dispose
        }

        internal static string GetTempFilePath(StorageEnvironmentOptions options, string name)
        {
            return options.TempPath.Combine(name + "_" + Guid.NewGuid() + StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.TempFileExtension).FullPath;
        }

        private Stream InitFileStream(StorageEnvironmentOptions options)
        {
            try
            {
                if (options.Encryption.IsEnabled)
                    return new TempCryptoStream(_fileTempPath);

                return SafeFileStream.Create(_fileTempPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.DeleteOnClose);
            }
            catch (IOException ioe) when (ioe.IsOutOfDiskSpaceException())
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
            catch (IOException ioe) when (ioe.IsOutOfDiskSpaceException())
            {
                ThrowDiskFullException();
            }
        }

        /// <summary>Random-access methods </summary>
        public override void Seek(long pos)
        {
            try
            {
                base.Seek(pos);
                _file.Seek(pos, SeekOrigin.Begin);
            }
            catch (IOException ioe) when (ioe.IsOutOfDiskSpaceException())
            {
                ThrowDiskFullException();
            }
        }

        public override long Length => _file.Length;

        public override void SetLength(long length)
        {
            try
            {
                _file.SetLength(length);
            }
            catch (IOException ioe) when (ioe.IsOutOfDiskSpaceException())
            {
                ThrowDiskFullException();
            }
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

                if (e is IOException ioe && e.IsOutOfDiskSpaceException())
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
            var folderPath = Path.GetDirectoryName(_fileTempPath);
            var driveInfo = DiskSpaceChecker.GetDiskSpaceInfo(folderPath);
            var freeSpace = driveInfo != null ? driveInfo.TotalFreeSpace.ToString() : "N/A";
            throw new DiskFullException($"There isn't enough space to flush the buffer of the file: {_fileTempPath}. " +
                                        $"Currently available space: {freeSpace}");
        }
    }
}
