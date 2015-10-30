using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using Voron.Debugging;
using Voron.Impl;
using Voron.Trees;

namespace Voron.Tests
{
    using System.Collections.Generic;

    public abstract class StorageTest : IDisposable
    {
        private StorageEnvironment _storageEnvironment;
        protected StorageEnvironmentOptions _options;
        protected const string DataDir = "test.data";

        public StorageEnvironment Env
        {
            get
            {
                if (_storageEnvironment == null)
                {
                    lock (this)
                    {
                        if (_storageEnvironment == null)
                            _storageEnvironment = new StorageEnvironment(_options);
                    }
                }
                return _storageEnvironment;
            }
        }

        protected StorageTest(StorageEnvironmentOptions options)
        {
            _options = options;
        }

        protected StorageTest()
        {
            DeleteDirectory(DataDir);
            _options = StorageEnvironmentOptions.CreateMemoryOnly();
            Configure(_options);
        }

        protected void RestartDatabase()
        {
            StopDatabase();

            StartDatabase();
        }

        protected void RequireFileBasedPager()
        {
            if(_storageEnvironment != null)
                throw new InvalidOperationException("Too late");
            if (_options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
                return;
            DeleteDirectory(DataDir);
            _options = StorageEnvironmentOptions.ForPath(DataDir);
            Configure(_options);
        }

        protected void StartDatabase()
        {
            _storageEnvironment = new StorageEnvironment(_options);
        }

        protected void StopDatabase()
        {
            var ownsPagers = _options.OwnsPagers;
            _options.OwnsPagers = false;

            _storageEnvironment.Dispose();

            _options.OwnsPagers = ownsPagers;
        }

        public static void DeleteDirectory(string dir)
        {
            if (Directory.Exists(dir) == false)
                return;

            for (int i = 0; i < 10; i++)
            {
                try
                {
                    Directory.Delete(dir, true);
                    return;
                }
                catch (DirectoryNotFoundException)
                {
                    return;
                }
                catch (Exception)
                {
                    Thread.Sleep(13);
                }
            }
            
            Directory.Delete(dir, true);
        }

        protected virtual void Configure(StorageEnvironmentOptions options)
        {

        }

        protected Stream StreamFor(string val)
        {
            return new MemoryStream(Encoding.UTF8.GetBytes(val));
        }

        public virtual void Dispose()
        {
            if (_storageEnvironment != null)
                _storageEnvironment.Dispose();
            _options.Dispose();
            DeleteDirectory(DataDir);

            _storageEnvironment = null;
            _options = null;
            GC.Collect(GC.MaxGeneration);
            GC.WaitForPendingFinalizers();
        }

        protected void RenderAndShow(Transaction tx, int showEntries = 25, string name = null)
        {
            if (name == null)
                RenderAndShow(tx, tx.Root);
            else
                RenderAndShow(tx, tx.Environment.CreateTree(tx,name));
        }

        protected void RenderAndShow(Transaction tx, Tree root)
        {
            if (Debugger.IsAttached == false)
                return;
            var rootPageNumber = tx.Environment.CreateTree(tx,root.Name).State.RootPageNumber;
            DebugStuff.RenderAndShow(root);

        }

        protected unsafe Tuple<Slice, Slice> ReadKey(Transaction tx, Slice key)
        {
            Lazy<Cursor> lazy;
            NodeHeader* node;
            var p = tx.Root.FindPageFor(key, out node, out lazy);
            

            if (node == null)
                return null;

            var item1 = p.GetNodeKey(node).ToSlice();

            if (item1.Compare(key) != 0)
                return null;
            return Tuple.Create(item1,
                new Slice((byte*) node + node->KeySize + Constants.NodeHeaderSize,
                    (ushort) node->DataSize));
        }

        protected IList<string> CreateTrees(StorageEnvironment env, int number, string prefix)
        {
            var results = new List<string>();

            using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (var i = 0; i < number; i++)
                {
                    results.Add(env.CreateTree(tx, prefix + i).Name);
                }

                tx.Commit();
            }

            return results;
        }
    }
}
