using System;
using System.Collections.Generic;
using System.IO;
using Lucene.Net.Store;
using Raven.Client.Util;
using Voron;
using Voron.Impl;

namespace Raven.Server.Indexing
{
    public unsafe class LuceneVoronDirectory : Lucene.Net.Store.Directory
    {
        private readonly StorageEnvironment _environment;

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

        public override bool FileExists(string name, IState s)
        {
            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));

            var filesTree = state.Transaction.ReadTree("Files");
            return filesTree.Read(name) != null;
        }

        public override string[] ListAll(IState s)
        {
            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));

            var files = new List<string>();
            var filesTree = state.Transaction.ReadTree("Files");
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

        public override long FileModified(string name, IState s)
        {
            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));

            var filesTree = state.Transaction.ReadTree("Files");
            Slice str;
            using (Slice.From(state.Transaction.Allocator, name, out str))
            {
                var info = filesTree.GetStreamInfo(str, writeable: false);
                if (info == null)
                    throw new FileNotFoundException(name);
                return info->Version;
            }
        }

        public override void TouchFile(string name, IState s)
        {
            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));

            var filesTree = state.Transaction.ReadTree("Files");
            var readResult = filesTree.Read(name);
            if (readResult == null)
                throw new FileNotFoundException("Could not find file", name);

            Slice str;
            using (Slice.From(state.Transaction.Allocator, name, out str))
            {
                if(filesTree.TouchStream(str) == 0)
                    throw new FileNotFoundException(name);
            }
        }

        public override long FileLength(string name, IState s)
        {
            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));

            var filesTree = state.Transaction.ReadTree("Files");
            Slice str;
            using (Slice.From(state.Transaction.Allocator, name, out str))
            {
                var info = filesTree.GetStreamInfo(str, writeable: false);
                if (info == null)
                    throw new FileNotFoundException(name);
                return info->TotalSize;
            }
        }

        public override void DeleteFile(string name, IState s)
        {
            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));

            var filesTree = state.Transaction.ReadTree("Files");
            var readResult = filesTree.ReadStream(name);
            if (readResult == null)
                throw new FileNotFoundException("Could not find file", name);

            filesTree.DeleteStream(name);
        }

        public override IndexInput OpenInput(string name, IState s)
        {
            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));

            return new VoronIndexInput(name, state.Transaction);
            
        }

        public override IndexOutput CreateOutput(string name, IState s)
        {
            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));

            return new VoronIndexOutput(_environment.Options, name, state.Transaction);
        }

        public IDisposable SetTransaction(Transaction tx, out IState state)
        {
            if (tx == null)
                throw new ArgumentNullException(nameof(tx));

            state = StateHolder.Current.Value = new VoronState(tx);

            return new DisposableAction(() =>
            {
                StateHolder.Current.Value = null;
            });
        }

        protected override void Dispose(bool disposing)
        {
        }
    }
}