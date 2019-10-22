using System;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10212 : RavenTestBase
    {
        public RavenDB_10212(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanSetRequestTimeoutInConventions()
        {
            var timeout1 = TimeSpan.FromSeconds(55);
            var timeout2 = TimeSpan.FromSeconds(33);

            using (var store = GetDocumentStore())
            {
                Assert.Null(store.Conventions.RequestTimeout);

                var executor = store.GetRequestExecutor();
                Assert.Null(executor.DefaultTimeout);
            }

            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore => documentStore.Conventions.RequestTimeout = timeout1
            }))
            {
                Assert.Equal(timeout1, store.Conventions.RequestTimeout);

                var executor = store.GetRequestExecutor();
                Assert.Equal(timeout1, executor.DefaultTimeout);

                using (store.SetRequestTimeout(timeout2))
                {
                    executor = store.GetRequestExecutor();
                    Assert.Equal(timeout2, executor.DefaultTimeout);

                    executor = store.GetRequestExecutor("other");
                    Assert.Equal(timeout1, executor.DefaultTimeout);
                }

                executor = store.GetRequestExecutor();
                Assert.Equal(timeout1, executor.DefaultTimeout);
            }
        }
    }
}
