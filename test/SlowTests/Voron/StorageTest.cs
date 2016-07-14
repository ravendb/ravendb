using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using FastTests;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl;
using Voron.Global;
using Sparrow;
using Sparrow.Logging;

namespace SlowTests.Voron
{
    public abstract class StorageTest : LinuxRaceConditionWorkAround, IDisposable
    {
        protected static readonly LoggerSetup NullLoggerSetup = new LoggerSetup(System.IO.Path.GetTempPath(), LogMode.None);

        private StorageEnvironment _storageEnvironment;
        protected StorageEnvironmentOptions _options;
        protected readonly string DataDir = GenerateDataDir();

        private ByteStringContext _allocator = new ByteStringContext();

        public static string GenerateDataDir()
        {
            var tempFileName = Path.GetTempFileName();
            File.Delete(tempFileName);
            tempFileName += "_dir";//avoid another concurrent call to GetTempFileName also creating it
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
                            _storageEnvironment = new StorageEnvironment(_options, NullLoggerSetup);
                    }
                }
                return _storageEnvironment;
            }
        }

        protected ByteStringContext Allocator
        {
            get { return _allocator; }
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
            _storageEnvironment = new StorageEnvironment(_options, NullLoggerSetup);
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

            var item1 = TreeNodeHeader.ToSlicePtr(txh.Allocator, node);

            if (SliceComparer.CompareInline(item1,key) != 0)
                return null;

            return Tuple.Create(item1, Slice.External( txh.Allocator, (byte*)node + node->KeySize + Constants.NodeHeaderSize, (ushort)node->DataSize, ByteStringType.Immutable));
        }
    }
}