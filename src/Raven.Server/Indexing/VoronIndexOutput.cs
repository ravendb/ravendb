﻿using System;
using System.IO;
using Lucene.Net.Store;
using Raven.Server.Logging;
using Raven.Server.Utils;
using Sparrow.Logging;
using Voron.Impl;
using Voron;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.Indexing
{
    public sealed class VoronIndexOutput : BufferedIndexOutput
    {
        private readonly RavenLogger _logger;

        private readonly TempFileCache _fileCache;
        private readonly string _name;
        private readonly string _tree;
        private readonly Transaction _tx;
        private Stream _file;
        private MemoryStream _ms;
        private readonly IndexOutputFilesSummary _indexOutputFilesSummary;

        private Stream StreamToUse => _ms ?? _file;

        public VoronIndexOutput(
            Index index,
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

            _logger = RavenLogManager.Instance.GetLoggerForIndex<VoronIndexOutput>(index);

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
                    ConvertMemoryStreamToFileStream();
                }

                _file.Write(b, offset, len);
            }
            catch (IOException ioe) when (ioe.IsOutOfDiskSpaceException())
            {
                ExceptionHelper.ThrowDiskFullException(_fileCache.FullPath);
            }
        }

        /// <summary>Random-access methods </summary>
        public override void Seek(long pos)
        {
            try
            {
                base.Seek(pos);

                StreamToUse.Seek(pos, SeekOrigin.Begin);
            }
            catch (IOException ioe) when (ioe.IsOutOfDiskSpaceException())
            {
                ExceptionHelper.ThrowDiskFullException(_fileCache.FullPath);
            }
        }

        public override long Length => StreamToUse.Length;

        public override void SetLength(long length)
        {
            try
            {
                if (_ms != null && _ms.Capacity < length)
                {
                    // too big, copy the buffer to the file
                    ConvertMemoryStreamToFileStream();
                }

                StreamToUse.SetLength(length);
            }
            catch (IOException ioe) when (ioe.IsOutOfDiskSpaceException())
            {
                ExceptionHelper.ThrowDiskFullException(_fileCache.FullPath);
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

        private void ConvertMemoryStreamToFileStream()
        {
            _file = _fileCache.RentFileStream();
            var position = _ms.Position;
            _ms.Position = 0;
            _ms.CopyTo(_file);
            _file.Position = position;
            _fileCache.ReturnMemoryStream(_ms);
            _ms = null;
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
                    StreamToUse.Seek(0, SeekOrigin.Begin);
                    files.AddStream(nameSlice, StreamToUse);
                }
            }
            catch (Exception e)
            {
                if (_logger.IsErrorEnabled)
                    _logger.Error($"Failed to copy the file: {_name}", e);

                _indexOutputFilesSummary.SetWriteError();

                if (e is IOException ioe && e.IsOutOfDiskSpaceException())
                {
                    // can happen when trying to copy from the file stream
                    ExceptionHelper.ThrowDiskFullException(_fileCache.FullPath);
                }

                throw;
            }
        }
    }
}
