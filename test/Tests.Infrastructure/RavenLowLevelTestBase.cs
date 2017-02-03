using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests
{
    public abstract class RavenLowLevelTestBase : IDisposable
    {
        private readonly ConcurrentSet<string> _pathsToDelete = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        protected static void WaitForIndexMap(Index index, long etag)
        {
            var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(15);
            Assert.True(SpinWait.SpinUntil(() => index.GetLastMappedEtagsForDebug().Values.Min() == etag, timeout));
        }

        private static int _counter;

        protected DocumentDatabase CreateDocumentDatabase([CallerMemberName] string caller = null, bool runInMemory = true, string dataDirectory = null, Action<RavenConfiguration> modifyConfiguration = null)
        {
            var name = caller != null ? $"{caller}_{Interlocked.Increment(ref _counter)}" : Guid.NewGuid().ToString("N");

            if (string.IsNullOrEmpty(dataDirectory))
                dataDirectory = NewDataPath(name);

            _pathsToDelete.Add(dataDirectory);

            var configuration = new RavenConfiguration(name, ResourceType.Database);
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory), int.MaxValue.ToString());
            configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened = true;

            modifyConfiguration?.Invoke(configuration);
            configuration.Initialize();

            configuration.Core.RunInMemory = runInMemory;
            configuration.Core.DataDirectory = new PathSetting(dataDirectory);

            var documentDatabase = new DocumentDatabase(name, configuration, null);
            documentDatabase.Initialize();

            return documentDatabase;
        }

        protected string NewDataPath([CallerMemberName]string prefix = null, bool forceCreateDir = false)
        {
            var path = RavenTestHelper.NewDataPath(prefix, 9999, forceCreateDir);

            _pathsToDelete.Add(path);
            return path;
        }

        public virtual void Dispose()
        {
            GC.SuppressFinalize(this);

            GC.Collect(2);
            GC.WaitForPendingFinalizers();

#pragma warning disable 618 // Yes, I know this is obselete
            var alreadyHasException = Marshal.GetExceptionCode() == 0;
#pragma warning restore 618
            var exceptionAggregator = new ExceptionAggregator("Could not dispose test");

            RavenTestHelper.DeletePaths(_pathsToDelete, exceptionAggregator);
            if (alreadyHasException == false)
                exceptionAggregator.ThrowIfNeeded();
        }

        protected static BlittableJsonReaderObject CreateDocument(JsonOperationContext context, string key, DynamicJsonValue value)
        {
            return context.ReadObject(value, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }
    }
}