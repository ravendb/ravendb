using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Lucene.Net.Store;
using Raven.Abstractions.Extensions;
using Voron;
using Voron.Data;
using Voron.Impl;
using Sparrow;

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
            var readResult = filesTree.Read(name);
            if (readResult == null)
                throw new FileNotFoundException("Could not find file", name);

            var fileInfo = *(LuceneFileInfo*)readResult.Reader.Base;

            return fileInfo.Version;
        }

        public override void TouchFile(string name)
        {
            var filesTree = _currentTransaction.Value.ReadTree("Files");
            var readResult = filesTree.Read(name);
            if (readResult == null)
                throw new FileNotFoundException("Could not find file", name);

            var fileInfo = *(LuceneFileInfo*)readResult.Reader.Base;
            fileInfo.Version++;

            var pos = filesTree.DirectAdd(name, readResult.Reader.Length);

            *(LuceneFileInfo*)pos = fileInfo;
        }

        public override long FileLength(string name)
        {
            var filesTree = _currentTransaction.Value.ReadTree("Files");
            var readResult = filesTree.Read(name);
            if (readResult == null)
                return 0;

            var fileInfo = *(LuceneFileInfo*)readResult.Reader.Base;

            return fileInfo.Length;
        }

        public override void DeleteFile(string name)
        {
            var filesTree = _currentTransaction.Value.ReadTree("Files");
            var readResult = filesTree.Read(name);
            if (readResult == null)
                throw new FileNotFoundException("Could not find file", name);

            filesTree.Delete(name);
            _currentTransaction.Value.DeleteTree(name);
        }

        public override IndexInput OpenInput(string name)
        {
            return new VoronIndexInput(_currentTransaction, name);
        }

        public override IndexOutput CreateOutput(string name)
        {
            var filesTree = _currentTransaction.Value.ReadTree("Files");

            var read = filesTree.Read(name);

            LuceneFileInfo fileInfo;

            if (read == null)
                fileInfo = new LuceneFileInfo();
            else
                fileInfo = *(LuceneFileInfo*)read.Reader.Base;

            return new VoronIndexOutput(_environment.Options.TempPath, name, _currentTransaction.Value, fileInfo);
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