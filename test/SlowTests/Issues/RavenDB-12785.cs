using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12785 : RavenTestBase
    {
        [Fact]
        public async Task CanUseOutputCollectionOnMapReduceJsIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var stream = GetDump("RavenDB-12785.ravendbdump"))
                {
                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions
                    {
                        TransformScript = @"
var collection = this['@metadata']['@collection'];
if(collection == 'ShoppingCarts')
   throw 'skip';
                    "
                    }, stream);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var errors = await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { "Events/ShoppingCart" }));
                    Assert.Empty(errors[0].Errors);
                    var count = await session.Advanced.AsyncRawQuery<object>("from ShoppingCarts").CountAsync();
                    Assert.Equal(1, count);
                }
            }
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_9912).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }
    }
}
