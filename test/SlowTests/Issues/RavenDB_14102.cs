using Tests.Infrastructure;
using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14102 : RavenTestBase
    {
        public RavenDB_14102(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public string Path;
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanEscapeLastCharInString(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenAsyncSession())
                {
                    await s.StoreAsync(new Item { Path = @"D:\Oren" }, "items/oren");
                    await s.SaveChangesAsync();
                }

                var op = await store.Operations.SendAsync(new PatchByQueryOperation(@"
from Items
update
{
    this.Path = this.Path.replace('D:\\', '/d/');
}
"));
                await op.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                using (var s = store.OpenAsyncSession())
                {
                    var i = await s.LoadAsync<Item>("items/oren");
                    Assert.Equal("/d/Oren", i.Path);
                }
            }
        }
    }
}
