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
using Sparrow.LowMemory;
using Sparrow.Threading;
using Xunit;

namespace SlowTests.Voron
{
    public abstract class StorageTest : LinuxRaceConditionWorkAround, IDisposable
    {
        private Lazy<StorageEnvironment> _storageEnvironment;
        public StorageEnvironment Env => _storageEnvironment.Value;

        protected StorageEnvironmentOptions Options;
        protected readonly string DataDir = GenerateTempDirectoryWithoutCollisions();

        private readonly ByteStringContext _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        protected ByteStringContext Allocator => _allocator;

        public static string GenerateTempDirectoryWithoutCollisions()
        {
            var tempPath = Path.GetTempPath();
            int attempt = 0;
            while (true)
            {
                string fileName = "RavenDB." + Path.GetRandomFileName();
                fileName = Path.Combine(tempPath, fileName);

                try
                {
                    Directory.CreateDirectory(fileName);
                    return fileName;
                }
                catch (IOException ex)
                {
                    if (++attempt == 10)
                        throw new IOException("Cannot create a unique temporary directory name.", ex);
                }
            }
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
            GC.KeepAlive(_storageEnvironment.Value); // force creation
        }

        protected void RequireFileBasedPager()
        {
            if (_storageEnvironment.IsValueCreated)
                throw new InvalidOperationException("Too late");
            if (Options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
                return;
            DeleteDirectory(DataDir);
            Options = StorageEnvironmentOptions.ForPath(DataDir);
            Configure(Options);
        }

        protected void StartDatabase()
        {
            _storageEnvironment = new Lazy<StorageEnvironment>(() => new StorageEnvironment(Options), LazyThreadSafetyMode.ExecutionAndPublication);
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
            DeleteDirectory(DataDir);

            _storageEnvironment = null;
            Options = null;
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

        protected unsafe Tuple<Slice, Slice> ReadKey(Transaction txh, Tree tree, Slice key) 
        {
            TreeNodeHeader* node;
            tree.FindPageFor(key, out node);
            
            if (node == null)
                return null;

            Slice item1;
            TreeNodeHeader.ToSlicePtr(txh.Allocator, node, out item1);

            if (SliceComparer.CompareInline(item1,key) != 0)
                return null;

            Slice item2;
            Slice.External(txh.Allocator, (byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize, (ushort) node->DataSize, ByteStringType.Immutable, out item2);
            return Tuple.Create(item1, item2);
        }

        public class TheoryAndSkipWhen32BitsEnvironment : TheoryAttribute
        {
            public TheoryAndSkipWhen32BitsEnvironment()
            {
                var shouldForceEnvVar = Environment.GetEnvironmentVariable("VORON_INTERNAL_ForceUsing32BitsPager");

                bool result;
                if (bool.TryParse(shouldForceEnvVar, out result))
                    if (result || IntPtr.Size == sizeof(int))
                        Skip = "Not supported for 32 bits";
                    
            }
        }
    }
}
