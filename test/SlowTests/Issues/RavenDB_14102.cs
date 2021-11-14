using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.JavaScript;
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
        [JavaScriptEngineClassData]
        public async Task CanEscapeLastCharInString(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
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
