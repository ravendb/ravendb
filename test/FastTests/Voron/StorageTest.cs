using Sparrow;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Threading;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl;
using Voron.Global;

namespace FastTests.Voron
{
    public abstract class StorageTest : LinuxRaceConditionWorkAround, IDisposable
    {
        private Lazy<StorageEnvironment> _storageEnvironment;
        public StorageEnvironment Env => _storageEnvironment.Value;

        protected StorageEnvironmentOptions Options;
        protected readonly string DataDir = GenerateDataDir();

        private ByteStringContext _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        protected ByteStringContext Allocator => _allocator;

        public static string GenerateDataDir()
        {
            var tempFileName = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(tempFileName);
            return tempFileName;
        }

        protected StorageTest(StorageEnvironmentOptions options)
        {
            Options = options;
            _storageEnvironment = new Lazy<StorageEnvironment>(() => new StorageEnvironment(Options), LazyThreadSafetyMode.ExecutionAndPublication);
        }

        protected StorageTest()
        {
            DeleteDirectory(DataDir);
            Options = StorageEnvironmentOptions.CreateMemoryOnly();

            Configure(Options);
            _storageEnvironment = new Lazy<StorageEnvironment>(() => new StorageEnvironment(Options), LazyThreadSafetyMode.ExecutionAndPublication);
        }

        protected void RestartDatabase()
        {
            StopDatabase();

            StartDatabase();
            GC.KeepAlive(_storageEnvironment.Value); // force initiliazation
        }

        protected void RequireFileBasedPager()
        {
            if(_storageEnvironment.IsValueCreated)
                throw new InvalidOperationException("Too late");
            if (Options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
                return;
            DeleteDirectory(DataDir);
            Options = StorageEnvironmentOptions.ForPath(DataDir);
            Configure(Options);
        }

        protected void StartDatabase()
        {
            // have to create a new instance
            _storageEnvironment = new Lazy<StorageEnvironment>(() => 
            new StorageEnvironment(Options), LazyThreadSafetyMode.ExecutionAndPublication);
        }

        protected void StopDatabase()
        {
            var ownsPagers = Options.OwnsPagers;
            Options.OwnsPagers = false;

            _storageEnvironment.Value.Dispose();

            Options.OwnsPagers = ownsPagers;
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

            try
            {
                Directory.Delete(dir, true);
            }
            catch (DirectoryNotFoundException)
            {
                
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Could not delete " + dir, e);
            }
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
            if (_storageEnvironment.IsValueCreated)
                _storageEnvironment.Value.Dispose();
            Options.Dispose();
            _allocator.Dispose();
            DeleteDirectory(DataDir);

            _storageEnvironment = null;
            Options = null;
            _allocator = null;
            GC.Collect(GC.MaxGeneration);
            GC.WaitForPendingFinalizers();
        }

        protected IList<string> CreateTrees(StorageEnvironment env, int number, string prefix)
        {
            var results = new List<string>();

            using (var tx = env.WriteTransaction())
            {
                for (var i = 0; i < number; i++)
                {
                    results.Add(tx.CreateTree(prefix + i).Name.ToString());
                }

                tx.Commit();
            }

            return results;
        }

        protected unsafe Tuple<Slice, Slice> ReadKey(Transaction txh, Tree tree, string key)
        {
            Slice s;
            Slice.From(txh.Allocator, key, out s);
            return ReadKey(txh, tree, s);
        }

        protected unsafe Tuple<Slice, Slice> ReadKey(Transaction txh, Tree tree, Slice key)
        {
            TreeNodeHeader* node;
            tree.FindPageFor(key, out node);

            if (node == null)
                return null;

            Slice item1;
            TreeNodeHeader.ToSlicePtr(txh.Allocator, node, out item1);

            if (!SliceComparer.Equals(item1,key))
                return null;
            Slice item2;
            Slice.External(txh.Allocator, (byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize, (ushort) node->DataSize, out item2);
            return Tuple.Create(item1, item2);
        }
    }
}
