using System;
using System.IO;
using Lucene.Net.Store;
using Raven.Server.ServerWide;
using Voron.Impl;
using Voron;

namespace Raven.Server.Indexing
{
    public class VoronIndexOutput : BufferedIndexOutput
    {
        public static readonly int MaxFileChunkSize = 128 * 1024 * 1024;

        private readonly string _name;
        private readonly Transaction _tx;
        private readonly Stream _file;

        public VoronIndexOutput(StorageEnvironmentOptions options, string name, Transaction tx)
        {
            _name = name;
            _tx = tx;
            var fileTempPath = Path.Combine(options.TempPath, name + "_" + Guid.NewGuid());
            //TODO: Pass this flag
            //const FileOptions FILE_ATTRIBUTE_TEMPORARY = (FileOptions)256;

            if (options.EncryptionEnabled)
                _file = new TempCryptoStream(fileTempPath);
            else
                _file = new FileStream(fileTempPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite, 
                    4096, FileOptions.DeleteOnClose);
        }

        public override void FlushBuffer(byte[] b, int offset, int len)
        {
            _file.Write(b, offset, len);
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
            base.Dispose(disposing);
            
            Slice nameSlice;

            var files = _tx.ReadTree("Files");

            using (Slice.From(_tx.Allocator, _name, out nameSlice))
            {
                _file.Seek(0, SeekOrigin.Begin);
                files.AddStream(nameSlice, _file);
            }
            
            _file.Dispose();
        }
    }
}