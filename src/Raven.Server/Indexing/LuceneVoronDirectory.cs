using System;
using System.Collections.Generic;
using System.IO;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Sparrow.Utils;
using Voron;
using Voron.Impl;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.Indexing
{
    public sealed unsafe class LuceneVoronDirectory : Lucene.Net.Store.Directory
    {
        private readonly Index _index;
        private readonly StorageEnvironment _environment;
        private readonly TempFileCache _tempFileCache;
        private readonly string _name;
        private readonly IndexOutputFilesSummary _indexOutputFilesSummary;
        private readonly LuceneIndexInputType _indexInputType;
        
        public string Name => _name;

        public long FilesAllocations => _indexOutputFilesSummary.TotalWritten;

        public string TempFullPath => _environment.Options.TempPath.FullPath;

        public LuceneVoronDirectory(Index index, Transaction tx, StorageEnvironment environment, TempFileCache tempFileCache, LuceneIndexInputType indexInputType) : this(index, tx, environment, tempFileCache, "Files", indexInputType)
        { }

        static LuceneVoronDirectory()
        {
            ArrayHolder.OnArrayHolderCreated = NativeMemory.IncrementLuceneManagedAllocations;
            ArrayHolder.OnArrayHolderDisposed = NativeMemory.DecrementLuceneManagedAllocations;
        }

        public LuceneVoronDirectory(Index index, Transaction tx, StorageEnvironment environment, TempFileCache tempFileCache, string name, LuceneIndexInputType indexInputType)
        {
            if (tx.IsWriteTransaction == false)
                throw new InvalidOperationException($"Creation of the {nameof(LuceneVoronDirectory)} must be done under a write transaction.");

            _index = index;
            _environment = environment;
            _tempFileCache = tempFileCache;
            _name = name;
            _indexInputType = indexInputType;
            
            SetLockFactory(NoLockFactory.Instance);

            tx.CreateTree(_name);

            _indexOutputFilesSummary = new IndexOutputFilesSummary();
        }

        public override bool FileExists(string name, IState s)
        {
            if (_indexOutputFilesSummary.HasVoronWriteErrors)
            {
                // we cannot modify the tx anymore 
                return false;
            }

            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));

            var filesTree = state.Transaction.ReadTree(_name);
            return filesTree.Exists(name);
        }

        public override string[] ListAll(IState s)
        {
            if (_indexOutputFilesSummary.HasVoronWriteErrors)
            {
                // we cannot modify the tx anymore 
                return new string[] { };
            }

            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));

            var files = new List<string>();
            var filesTree = state.Transaction.ReadTree(_name);
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
            if (_indexOutputFilesSummary.HasVoronWriteErrors)
            {
                // we cannot modify the tx anymore 
                return -1;
            }

            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));

            var filesTree = state.Transaction.ReadTree(_name);
            using (Slice.From(state.Transaction.Allocator, name, out Slice str))
            {
                var info = filesTree.GetStreamInfo(str, writable: false);
                if (info == null)
                    throw new FileNotFoundException(name);

                return info->Version;
            }
        }

        public override void TouchFile(string name, IState s)
        {
            if (_indexOutputFilesSummary.HasVoronWriteErrors)
            {
                // we cannot modify the tx anymore 
                return;
            }

            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));

            var filesTree = state.Transaction.ReadTree(_name);
            if (filesTree.Exists(name) == false)
                throw new FileNotFoundException("Could not find file", name);

            using (Slice.From(state.Transaction.Allocator, name, out Slice str))
            {
                if (filesTree.TouchStream(str) == 0)
                    throw new FileNotFoundException(name);
            }
        }

        public override long FileLength(string name, IState s)
        {
            if (_indexOutputFilesSummary.HasVoronWriteErrors)
            {
                // we cannot modify the tx anymore 
                return -1;
            }

            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));

            var filesTree = state.Transaction.ReadTree(_name);
            using (Slice.From(state.Transaction.Allocator, name, out Slice str))
            {
                var info = filesTree.GetStreamInfo(str, writable: false);
                if (info == null)
                    throw new FileNotFoundException(name);
                return info->TotalSize;
            }
        }

        public override void DeleteFile(string name, IState s)
        {
            if (_indexOutputFilesSummary.HasVoronWriteErrors)
            {
                // we cannot modify the tx anymore 
                return;
            }

            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));

            var filesTree = state.Transaction.ReadTree(_name);
            if (filesTree.Exists(name) == false)
                throw new FileNotFoundException("Could not find file", name);

            filesTree.DeleteStream(name);
        }

        public override IndexInput OpenInput(string name, IState s)
        {
            if (_indexOutputFilesSummary.HasVoronWriteErrors)
                throw new InvalidOperationException($"Tried to create an index input for: '{name}' while we have Voron write errors");

            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));

            return _indexInputType switch
            {
                LuceneIndexInputType.Buffered => new VoronBufferedInput(this, name, state.Transaction, _name),
                LuceneIndexInputType.Standard => new VoronIndexInput(this, name, state.Transaction, _name),
                _ => throw new ArgumentException(
                    $"Cannot create {nameof(IndexInput)} implemented as {_indexInputType} inside {nameof(LuceneVoronDirectory)} because it is not added to a list.")
            };
        }

        public override IndexOutput CreateOutput(string name, IState s)
        {
            if (_indexOutputFilesSummary.HasVoronWriteErrors)
                throw new InvalidOperationException($"Tried to create an index output for: '{name}' while we have Voron write errors");

            var state = s as VoronState;
            if (state == null)
                throw new ArgumentNullException(nameof(s));
            
            return new VoronIndexOutput(_index, _tempFileCache, name, state.Transaction, _name, _indexOutputFilesSummary);
        }

        public IDisposable SetTransaction(Transaction tx, out IState state)
        {
            if (tx == null)
                throw new ArgumentNullException(nameof(tx));

            var currentValue = new VoronState(tx);
            state = StateHolder.Current.Value = currentValue;

            return new DisposableAction(() =>
            {
                currentValue.Transaction = null;
                StateHolder.Current.Value = null;
            });
        }

        public void ResetAllocations()
        {
            _indexOutputFilesSummary.Reset();
        }
    }
}
