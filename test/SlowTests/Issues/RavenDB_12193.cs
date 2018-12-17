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
        [Fact(Skip = "TaskScheduler.UnobservedTaskException registers globally so running the full test suite causes unexpected failures here")]
        public void Should_Throw_On_UnobservedTaskException()
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
                GC.WaitForPendingFinalizers();

                Assert.Equal(0, count);
            }
            finally
            {
                TaskScheduler.UnobservedTaskException -= task;
            }
        }
    }
}
