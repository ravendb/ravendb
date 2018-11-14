using System;
using System.IO;
using Lucene.Net.Store;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Utils;
using Voron.Impl;
using Voron;

namespace Raven.Server.Indexing
{
    public class VoronIndexOutput : BufferedIndexOutput
    {
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

            if (options.EncryptionEnabled)
                _file = new TempCryptoStream(_fileTempPath);
            else
                _file = SafeFileStream.Create(_fileTempPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.DeleteOnClose);

            _tx.ReadTree(_tree).AddStream(name, Stream.Null); // ensure it's visible by LuceneVoronDirectory.FileExists, the actual write is inside Dispose
        }

        public override void FlushBuffer(byte[] b, int offset, int len)
        {
            _file.Write(b, offset, len);
            _indexOutputFilesSummary.Increment(len);
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

                var files = _tx.ReadTree(_tree);

                using (Slice.From(_tx.Allocator, _name, out var nameSlice))
                {
                    _file.Seek(0, SeekOrigin.Begin);
                    files.AddStream(nameSlice, _file);
                }
            }
            finally
            {
                _file.Dispose();
                PosixFile.DeleteOnClose(_fileTempPath);
            }
        }
    }
}
