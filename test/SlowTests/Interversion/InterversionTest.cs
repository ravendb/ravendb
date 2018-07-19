using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Interversion
{
    public class InterversionTest : InterversionTestBase
    {
        [Fact]
        public async void Test()
        {
            var getStoreTask405 = GetDocumentStoreAsync("4.0.5");
            var getStoreTask406patch = GetDocumentStoreAsync("4.0.6-patch-40047");
            var getStoreTaskCurrent = GetDocumentStoreAsync();
            Task.WaitAll(getStoreTask405, getStoreTask406patch, getStoreTaskCurrent);
        }

    }
}
