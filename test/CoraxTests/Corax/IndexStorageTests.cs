using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Corax;
using Raven.Server.Utils;
using Voron;
using Xunit.Abstractions;

namespace CoraxTests
{
    public abstract class IndexStorageTests : LinuxRaceConditionWorkAround, IDisposable
    {
        private Lazy<IndexingEnvironment> _indexStorageEnvironment;
        public IndexingEnvironment Env => _indexStorageEnvironment.Value;
        public StorageEnvironmentOptions Options { get; private set; }

        protected readonly string DataDir = RavenTestHelper.NewDataPath(nameof(IndexStorageTests), 0);


        protected IndexStorageTests(StorageEnvironmentOptions options, ITestOutputHelper output) : base(output)
        {
            Options = options;
            _indexStorageEnvironment = new Lazy<IndexingEnvironment>(() => new IndexingEnvironment(new StorageEnvironment(options)), LazyThreadSafetyMode.ExecutionAndPublication);
        }

        protected IndexStorageTests(ITestOutputHelper output) : base(output)
        {
            IOExtensions.DeleteDirectory(DataDir);
            
            // This will create a memory only instance for the Voron instance.
            Options = StorageEnvironmentOptions.CreateMemoryOnly();

            Configure(Options);
            _indexStorageEnvironment = new Lazy<IndexingEnvironment>(() => new IndexingEnvironment(new StorageEnvironment(Options)), LazyThreadSafetyMode.ExecutionAndPublication);
        }

        protected virtual void Configure(StorageEnvironmentOptions options)
        {

        }

        protected void RequireFileBasedPager()
        {
            if (_indexStorageEnvironment.IsValueCreated)
                throw new InvalidOperationException("Too late");
            if (Options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
                return;

            IOExtensions.DeleteDirectory(DataDir);
            Options = StorageEnvironmentOptions.ForPath(DataDir);
            Configure(Options);
        }

        protected void StartDatabase()
        {
            _indexStorageEnvironment = new Lazy<IndexingEnvironment>(() => new IndexingEnvironment(new StorageEnvironment(Options)), LazyThreadSafetyMode.ExecutionAndPublication);
            GC.KeepAlive(_indexStorageEnvironment.Value); // force creation
        }

        protected void StopDatabase(bool shouldDisposeOptions = false)
        {
            var ownsPagers = Options.OwnsPagers;

            Options.OwnsPagers = shouldDisposeOptions;

            _indexStorageEnvironment.Value.Dispose();

            Options.OwnsPagers = ownsPagers;
        }


        public override void Dispose()
        {
            base.Dispose();

            var aggregator = new ExceptionAggregator("Could not dispose Index Storage test.");

            aggregator.Execute(() =>
            {
                if (_indexStorageEnvironment.IsValueCreated)
                    _indexStorageEnvironment.Value.Dispose();

                _indexStorageEnvironment = null;
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
    }
}
