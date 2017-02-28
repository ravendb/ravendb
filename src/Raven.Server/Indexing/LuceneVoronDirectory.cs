using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Lucene.Net.Store;
using Raven.Client.Util;
using Voron;
using Voron.Impl;
using Voron.Data.Fixed;

namespace Raven.Server.Indexing
{
    public unsafe class LuceneVoronDirectory : Lucene.Net.Store.Directory
    {
        private readonly StorageEnvironment _environment;

        private readonly AsyncLocal<Transaction> _currentTransaction = new AsyncLocal<Transaction>();

        public LuceneVoronDirectory(StorageEnvironment environment)
        {
            _environment = environment;
            base.SetLockFactory(NoLockFactory.Instance);
            using (var tx = _environment.WriteTransaction())
            {
                tx.CreateTree("Files");
                tx.Commit();
            }
        }

        public override bool FileExists(string name)
        {
            var filesTree = _currentTransaction.Value.ReadTree("Files");
            return filesTree.Read(name) != null;
        }

        public override string[] ListAll()
        {
            var files = new List<string>();
            var filesTree = _currentTransaction.Value.ReadTree("Files");
            using (var it = filesTree.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        files.Add(it.CurrentKey.ToString());
                    } while (it.MoveNext());
                }
            }
            return files.ToArray();
        }

        public override long FileModified(string name)
        {
            var filesTree = _currentTransaction.Value.ReadTree("Files");
            Slice str;
            using (Slice.From(_currentTransaction.Value.Allocator, name, out str))
            {
                long length;
                int version;
                FixedSizeTree _;
                filesTree.GetStreamLengthAndVersion(str, out length, out version, out _);
                if (length == -1)
                    throw new FileNotFoundException(name);
                return version;
            }
        }

        public override void TouchFile(string name)
        {
            var filesTree = _currentTransaction.Value.ReadTree("Files");
            var readResult = filesTree.Read(name);
            if (readResult == null)
                throw new FileNotFoundException("Could not find file", name);

            Slice str;
            using (Slice.From(_currentTransaction.Value.Allocator, name, out str))
            {
                if(filesTree.TouchStream(str) == 0)
                    throw new FileNotFoundException(name);
            }
        }

        public override long FileLength(string name)
        {
            var filesTree = _currentTransaction.Value.ReadTree("Files");
            Slice str;
            using (Slice.From(_currentTransaction.Value.Allocator, name, out str))
            {
                long length;
                int version;
                FixedSizeTree _;
                filesTree.GetStreamLengthAndVersion(str, out length, out version, out _);
                if(length == -1)
                    throw new FileNotFoundException(name);
                return length;
            }
        }

        public override void DeleteFile(string name)
        {
            var filesTree = _currentTransaction.Value.ReadTree("Files");
            var readResult = filesTree.ReadStream(name);
            if (readResult == null)
                throw new FileNotFoundException("Could not find file", name);

            filesTree.DeleteStream(name);
        }

        public override IndexInput OpenInput(string name)
        {
            return new VoronIndexInput(_currentTransaction, name);
            
        }

        public override IndexOutput CreateOutput(string name)
        {
            return new VoronIndexOutput(_environment.Options.TempPath, name, _currentTransaction.Value);
        }

        public IDisposable SetTransaction(Transaction tx)
        {
            if (tx == null) throw new ArgumentNullException(nameof(tx));
            _currentTransaction.Value = tx;

            return new DisposableAction(() => _currentTransaction.Value = null);
        }

        protected override void Dispose(bool disposing)
        {
        }
    }
}