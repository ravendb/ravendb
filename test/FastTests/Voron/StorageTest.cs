using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using Raven.Server.Utils;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl;
using Xunit.Abstractions;
using Constants = Voron.Global.Constants;

namespace FastTests.Voron
{
    public abstract class StorageTest : LinuxRaceConditionWorkAround, IDisposable
    {
        private Lazy<StorageEnvironment> _storageEnvironment;
        public StorageEnvironment Env => _storageEnvironment.Value;

        protected StorageEnvironmentOptions Options;
        protected readonly string DataDir = RavenTestHelper.NewDataPath(nameof(StorageTest), 0);

        protected ByteStringContext Allocator { get; } = new ByteStringContext(SharedMultipleUseFlag.None);

        protected StorageTest(StorageEnvironmentOptions options, ITestOutputHelper output) : base(output)
        {
            Options = options;
            _storageEnvironment = new Lazy<StorageEnvironment>(() => new StorageEnvironment(Options), LazyThreadSafetyMode.ExecutionAndPublication);
        }

        protected StorageTest(ITestOutputHelper output) : base(output)
        {
            IOExtensions.DeleteDirectory(DataDir);
            Options = StorageEnvironmentOptions.CreateMemoryOnly();

            Configure(Options);
            _storageEnvironment = new Lazy<StorageEnvironment>(() => new StorageEnvironment(Options), LazyThreadSafetyMode.ExecutionAndPublication);
        }

        protected void RestartDatabase()
        {
            var isFileBasedEnv = Options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions;

            StopDatabase(shouldDisposeOptions: isFileBasedEnv);

            if (isFileBasedEnv)
            {
                Options = StorageEnvironmentOptions.ForPath(DataDir);
                Configure(Options);
            }

            StartDatabase();
        }

        protected void RequireFileBasedPager()
        {
            if (_storageEnvironment.IsValueCreated)
                throw new InvalidOperationException("Too late");
            if (Options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
                return;

            IOExtensions.DeleteDirectory(DataDir);
            Options = StorageEnvironmentOptions.ForPath(DataDir);
            Configure(Options);
        }

        protected void StartDatabase()
        {
            _storageEnvironment = new Lazy<StorageEnvironment>(() => new StorageEnvironment(Options), LazyThreadSafetyMode.ExecutionAndPublication);
            GC.KeepAlive(_storageEnvironment.Value); // force creation
        }

        protected void StopDatabase(bool shouldDisposeOptions = false)
        {
            var ownsPagers = Options.OwnsPagers;

            Options.OwnsPagers = shouldDisposeOptions;

            _storageEnvironment.Value.Dispose();

            Options.OwnsPagers = ownsPagers;
        }

        protected virtual void Configure(StorageEnvironmentOptions options)
        {

        }

        protected Stream StreamFor(string val)
        {
            return new MemoryStream(Encoding.UTF8.GetBytes(val));
        }

        public override void Dispose()
        {
            base.Dispose();

            var aggregator = new ExceptionAggregator("Could not dispose Storage test.");

            aggregator.Execute(() =>
            {
                if (_storageEnvironment.IsValueCreated)
                    _storageEnvironment.Value.Dispose();

                _storageEnvironment = null;
            });

            aggregator.Execute(() =>
            {
                Options?.Dispose();
                Options = null;
            });

            aggregator.Execute(() =>
            {
                IOExtensions.DeleteDirectory(DataDir);
            });

            aggregator.ThrowIfNeeded();

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

        protected Tuple<Slice, Slice> ReadKey(Transaction txh, Tree tree, string key)
        {
            using (Slice.From(txh.Allocator, key, out var s))
                return ReadKey(txh, tree, s);
        }

        protected unsafe Tuple<Slice, Slice> ReadKey(Transaction txh, Tree tree, Slice key)
        {
            tree.FindPageFor(key, out var node);

            if (node == null)
                return null;

            TreeNodeHeader.ToSlicePtr(txh.Allocator, node, out var item1);

            if (SliceComparer.CompareInline(item1, key) != 0)
                return null;

            Slice.External(txh.Allocator, (byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize, (ushort)node->DataSize, ByteStringType.Immutable, out var item2);
            return Tuple.Create(item1, item2);
        }


    }
}
