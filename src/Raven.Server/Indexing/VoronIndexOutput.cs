using System;
using System.IO;
using System.Threading;
using Lucene.Net.Store;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Server.Exceptions;
using Sparrow.Server.Utils;
using Voron.Impl;
using Voron;

namespace Raven.Server.Indexing
{
    public class VoronIndexOutput : BufferedIndexOutput
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LuceneVoronDirectory>("VoronIndexOutput");

        private readonly TempFileCache _fileCache;
        private readonly string _name;
        private readonly string _tree;
        private readonly Transaction _tx;
        private Stream _file;
        private MemoryStream _ms;
        private readonly IndexOutputFilesSummary _indexOutputFilesSummary;

        public VoronIndexOutput(
            TempFileCache fileCache,
            string name,
            Transaction tx,
            string tree,
            IndexOutputFilesSummary indexOutputFilesSummary)
        {
            _fileCache = fileCache;
            _name = name;
            _tree = tree;
            _tx = tx;
            _indexOutputFilesSummary = indexOutputFilesSummary;

            _ms = fileCache.RentMemoryStream();
            _tx.ReadTree(_tree).AddStream(name, Stream.Null); // ensure it's visible by LuceneVoronDirectory.FileExists, the actual write is inside Dispose
        }


        public override void FlushBuffer(byte[] b, int offset, int len)
        {
            try
            {
                _indexOutputFilesSummary.Increment(len);
                if (_ms != null)
                {
                    if (_ms.Capacity - _ms.Position - len >= 0)
                    {
                        _ms.Write(b, offset, len);
                        return;
                    }
                    // too big, copy the buffer to the file
                    _file = _fileCache.RentFileStream();
                    var position = _ms.Position;
                    _ms.Position = 0;
                    _ms.CopyTo(_file);
                    _file.Position = position;
                    _fileCache.ReturnMemoryStream(_ms);
                    _ms = null;
                }
                _file.Write(b, offset, len);
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
                if (_ms != null)
                    _ms.Seek(pos, SeekOrigin.Begin);
                else
                    _file.Seek(pos, SeekOrigin.Begin);
            }
            catch (IOException ioe) when (ioe.IsOutOfDiskSpaceException())
            {
                ThrowDiskFullException();
            }
        }

        public override long Length => _ms?.Length ?? _file.Length;

        public override void SetLength(long length)
        {
            try
            {
                if (_ms != null)
                    _ms.SetLength(length);
                else
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
                if (_ms != null)
                    _fileCache.ReturnMemoryStream(_ms);
                _ms = null;
                if (_file != null)
                    _fileCache.ReturnFileStream(_file);
                _file = null;
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
                    if (_ms != null)
                    {
                        _ms.Seek(0, SeekOrigin.Begin);
                        files.AddStream(nameSlice, _ms);
                    }
                    else
                    {
                        _file.Seek(0, SeekOrigin.Begin);
                        files.AddStream(nameSlice, _file);
                    }
                }
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to copy the file: {_name}", e);

                _indexOutputFilesSummary.SetWriteError();

                if (e is IOException ioe && e.IsOutOfDiskSpaceException())
                {
                    // can happen when trying to copy from the file stream
                    ThrowDiskFullException();
                }

                throw;
            }
        }

        private void ThrowDiskFullException()
        {
            var folderPath = _fileCache.FullPath;
            var driveInfo = DiskSpaceChecker.GetDiskSpaceInfo(folderPath);
            var freeSpace = driveInfo != null ? driveInfo.TotalFreeSpace.ToString() : "N/A";
            throw new DiskFullException($"There isn't enough space to flush the buffer in: {folderPath}. " +
                                        $"Currently available space: {freeSpace}");
        }
    }
}
