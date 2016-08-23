using System;
using System.IO;
using Lucene.Net.Store;
using Voron.Impl;
using Voron;
using Sparrow;

namespace Raven.Server.Indexing
{
    public class VoronIndexOutput : BufferedIndexOutput
    {
        public static readonly Slice DataKey = Slice.From(StorageEnvironment.LabelsContext, "_", ByteStringType.Immutable);

        private readonly string _name;
        private readonly Transaction _tx;
        private readonly FileStream _file;

        public VoronIndexOutput(string tempPath, string name, Transaction tx)
        {
            _name = name;
            _tx = tx;
            var fileTempPath = Path.Combine(tempPath, name + "_" + Guid.NewGuid());
            //TODO: Pass this flag
            //const FileOptions FILE_ATTRIBUTE_TEMPORARY = (FileOptions)256;
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
            var tree = _tx.CreateTree(_name);
            _file.Seek(0, SeekOrigin.Begin);
            tree.Add(DataKey, _file);
        }
    }
}