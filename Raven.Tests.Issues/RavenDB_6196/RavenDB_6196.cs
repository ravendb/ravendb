// -----------------------------------------------------------------------
//  <copyright file="RavenDB_6196.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues.RavenDB_6196
{
    public class RavenDB_6196 : RavenTestBase
    {
        [Theory]
        [InlineData("Raven.Tests.Issues.RavenDB_6196.Smuggler.Northwind_4.0.40008.ravendbdump")]
        public async Task CanImportNorthwind(string file)
        {
            using (var stream = GetType().GetTypeInfo().Assembly.GetManifestResourceStream(file))
            {
                Assert.NotNull(stream);

                using (var store = NewRemoteDocumentStore())
                {
                    var smuggler = new SmugglerDatabaseApi();
                    await smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions>
                    {
                        FromStream = stream,
                        To = new RavenConnectionStringOptions
                        {
                            Url = store.Url,
                            DefaultDatabase = store.DefaultDatabase
                        }
                    });

                    var stats = store.DatabaseCommands.GetStatistics();
                    Assert.Equal(1059, stats.CountOfDocuments);
                    Assert.Equal(0 + 1, stats.CountOfIndexes); // +1 default index
                    Assert.Equal(0, stats.CountOfResultTransformers);

                    var collections = new Dictionary<string, long>();
                    var enumerator = store.DatabaseCommands.StreamDocs(fromEtag: Etag.Empty);
                    while (enumerator.MoveNext())
                    {
                        var metadata = (RavenJObject)enumerator.Current[Constants.Metadata];
                        var collection = metadata.Value<string>(Constants.RavenEntityName) ?? Constants.SystemDatabase;

                        if (collections.ContainsKey(collection) == false)
                            collections[collection] = 0;

                        collections[collection] += 1;
                    }

                    Assert.Equal(9, collections.Count);
                    Assert.Equal(8, collections["Categories"]);
                    Assert.Equal(91, collections["Companies"]);
                    Assert.Equal(9, collections["Employees"]);
                    Assert.Equal(830, collections["Orders"]);
                    Assert.Equal(77, collections["Products"]);
                    Assert.Equal(4, collections["Regions"]);
                    Assert.Equal(3, collections["Shippers"]);
                    Assert.Equal(29, collections["Suppliers"]);
                    Assert.True(collections.ContainsKey(Constants.SystemDatabase));

                    var document = store.DatabaseCommands.Get("orders/1");

                    Assert.True(document.Metadata.ContainsKey("Raven-Entity-Name"));
                    Assert.True(document.Metadata.ContainsKey("Raven-Last-Modified"));
                    Assert.True(document.Metadata.ContainsKey("Last-Modified"));
                    Assert.False(document.Metadata.ContainsKey("@collection"));
                    Assert.False(document.Metadata.ContainsKey("@index-score"));
                    Assert.False(document.Metadata.ContainsKey("@last-modified"));
                }
            }
        }
    }
}