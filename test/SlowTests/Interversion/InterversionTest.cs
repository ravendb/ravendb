using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Interversion
{
    public class InterversionTest : InterversionTestBase
    {
        [Fact]
        public async Task Test()
        {
            var getStoreTask405 = GetDocumentStoreAsync("4.0.5");
            var getStoreTask406patch = GetDocumentStoreAsync("4.0.6-patch-40047");
            var getStoreTaskCurrent = GetDocumentStoreAsync();
            
            await Task.WhenAll(getStoreTask405, getStoreTask406patch, getStoreTaskCurrent);

            AssertStore(await getStoreTask405);
            AssertStore(await getStoreTask406patch);
            AssertStore(await getStoreTaskCurrent);
        }

        private static void AssertStore(Raven.Client.Documents.IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Company()
                {
                    Name = "HR"
                }, "companies/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var c = session.Load<Company>("companies/1");
                Assert.NotNull(c);
                Assert.Equal("HR", c.Name);
            }
        }
    }
}
