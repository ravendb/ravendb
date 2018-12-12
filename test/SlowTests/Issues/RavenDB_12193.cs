using System;
using Xunit;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Changes;
using Raven.Client.Extensions;
using Raven.Tests.Core.Utils.Entities;

namespace SlowTests.Issues
{
    public class RavenDB_12193 : RavenTestBase
    {
        // RavenDB-12481
        [Fact]
        public async Task Should_Throw_On_UnobservedTaskException()
        {
            var count = 0;

            EventHandler<UnobservedTaskExceptionEventArgs> task = (sender, args) =>
            {
                Interlocked.Increment(ref count);
            };

            try
            {
                TaskScheduler.UnobservedTaskException += task;
                using (GetDocumentStore().Changes().ForAllDocuments().Subscribe(change => {{}})){}

                GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);

                await Task.Delay(1000);
                Assert.Equal(0, count);
            }
            finally
            {
                TaskScheduler.UnobservedTaskException -= task;
            }
        }
    }
}
