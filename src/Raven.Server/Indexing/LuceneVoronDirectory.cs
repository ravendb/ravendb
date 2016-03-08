using System.Collections.Generic;
using System.IO;
using System.Threading;
using Lucene.Net.Store;
using Voron;
using Voron.Impl;

namespace Raven.Server.Indexing
{
    public class LuceneVoronDirectory : Lucene.Net.Store.Directory
    {
        private readonly StorageEnvironment _environment;

        public ThreadLocal<Transaction> CurrentTransaction = new ThreadLocal<Transaction>();

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
            var filesTree = CurrentTransaction.Value.ReadTree("Files");
            return filesTree.ReadVersion(name) != 0;
        }

        public override string[] ListAll()
        {
            var files = new List<string>();
            var filesTree = CurrentTransaction.Value.ReadTree("Files");
            using (var it = filesTree.Iterate())
            {
                if (it.Seek(Slice.BeforeAllKeys))
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
            var filesTree = CurrentTransaction.Value.ReadTree("Files");
            var readResult = filesTree.Read(name);
            if(readResult == null)
                throw new FileNotFoundException("Could not find file", name);
            return readResult.Version;
        }

        public override unsafe void TouchFile(string name)
        {
            var filesTree = CurrentTransaction.Value.ReadTree("Files");
            Slice key = name;
            var readResult = filesTree.Read(key);
            if (readResult == null)
                throw new FileNotFoundException("Could not find file", name);
            filesTree.DirectAdd(key, readResult.Reader.Length);
        }

        public override long FileLength(string name)
        {
            var filesTree = CurrentTransaction.Value.ReadTree("Files");
            Slice key = name;
            var readResult = filesTree.Read(key);
            if (readResult == null)
                throw new FileNotFoundException("Could not find file", name);

            //TODO: handle files larger than 2GB
            return readResult.Reader.Length;
        }

        public override void DeleteFile(string name)
        {
            var filesTree = CurrentTransaction.Value.ReadTree("Files");
            Slice key = name;
            var readResult = filesTree.Read(key);
            if (readResult == null)
                throw new FileNotFoundException("Could not find file", name);
            filesTree.Delete(key);
        }

        public override unsafe IndexInput OpenInput(string name)
        {
            var filesTree = CurrentTransaction.Value.ReadTree("Files");
            Slice key = name;
            var readResult = filesTree.Read(key);
            if (readResult == null)
                throw new FileNotFoundException("Could not find file", name);

            return new VoronIndexInput(readResult.Reader.Base, readResult.Reader.Length);
        }

        public override IndexOutput CreateOutput(string name)
        {
            //TODO: _environment.Options.TempPath
            return new VoronIndexOutput(Path.GetTempPath(),name, CurrentTransaction.Value);
        }

        protected override void Dispose(bool disposing)
        {
            
        }
    }
}