using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using FastTests;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl;

namespace SlowTests.Voron
{
    public abstract class StorageTest : LinuxRaceConditionWorkAround, IDisposable
    {
        private StorageEnvironment _storageEnvironment;
        protected StorageEnvironmentOptions _options;
        protected readonly string DataDir = GenerateDataDir();

        public static string GenerateDataDir()
        {
            var tempFileName = Path.GetTempFileName();
            File.Delete(tempFileName);
            Directory.CreateDirectory(tempFileName);
            return tempFileName;
        }

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
            if (_storageEnvironment != null)
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
            if (_storageEnvironment != null)
                _storageEnvironment.Dispose();
            _options.Dispose();
            DeleteDirectory(DataDir);

            _storageEnvironment = null;
            _options = null;
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
                    results.Add(tx.CreateTree(prefix + i).Name);
                }

                tx.Commit();
            }

            return results;
        }

        protected unsafe Tuple<Slice, Slice> ReadKey(Transaction txh, Tree tree, Slice key)
        {
            TreeNodeHeader* node;
            var p = tree.FindPageFor(key, out node);


            if (node == null)
                return null;

            var item1 = p.GetNodeKey(node).ToSlice();

            if (item1.Compare(key) != 0)
                return null;
            return Tuple.Create(item1,
                new Slice((byte*)node + node->KeySize + Constants.NodeHeaderSize,
                    (ushort)node->DataSize));
        }
    }
}