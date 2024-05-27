using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class JavaScriptIndexTests : RavenTestBase
    {
        public JavaScriptIndexTests(ITestOutputHelper output) : base(output)
        {
        }

        private class Company
        {
            public string Name { get; set; }
            public int Fax { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanUseIdMethodInJavascriptIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new JavaScriptIndexWithIdMethod().Execute(store);

                using (var s = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 20; i++)
                    {
                        await s.StoreAsync(new Company
                        {
                            Name = $"{i}",
                            Fax = i
                        });
                        await s.SaveChangesAsync();

                    }

                }


                Indexes.WaitForIndexing(store);
                using (var s = store.OpenAsyncSession())
                {
                    var ids = await s.Query<JavaScriptIndexWithIdMethod.Result, JavaScriptIndexWithIdMethod>().ToListAsync();

                    Assert.Equal(10, ids.Count);

                    for (var i = 0; i < 10; i++)
                    {
                        Assert.NotNull(ids[i].Id);
                        Assert.Equal($"{i}", ids[i].Name);
                        Assert.Equal(i, ids[i].Fax);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]        
        public async Task CanUseGetMetadataMethodInJavascriptIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new JavaScriptIndexWithGetMetadataMethod().Execute(store);

                using (var s = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 20; i++)
                    {
                        await s.StoreAsync(new Company
                        {
                            Name = $"{i}",
                            Fax = i
                        });
                    }

                    await s.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);
                using (var s = store.OpenAsyncSession())
                {
                    var ids = await s.Query<Company, JavaScriptIndexWithGetMetadataMethod>().ProjectInto<JavaScriptIndexWithGetMetadataMethod.Result>().ToListAsync();
                    Assert.Equal(10, ids.Count);
                    for (var i = 0; i < 10; i++)
                    {
                        Assert.Equal("Companies", ids[i].Collection);
                        Assert.NotNull(ids[i].ChangeVector);
                        Assert.NotNull(ids[i].Id);
                        Assert.NotNull(ids[i].LastModified);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.Single)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanAllowStringCompilation(Options options, bool allowStringCompilation)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new MyJSIndex(allowStringCompilation);
                await index.ExecuteAsync(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company
                    {
                        Name = "RavenDB"
                    });
                    await session.SaveChangesAsync();
                }

                await Indexes.WaitForIndexingAsync(store);

                if (allowStringCompilation)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var result = await session.Query<Company, MyJSIndex>()
                            .ProjectInto<MyJSIndex.CustomProjection>()
                            .ToListAsync();

                        Assert.Equal(1, result.Count);
                        Assert.Equal("Hello World", result[0].NewName);
                    }
                }
                else
                {
                    var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation([index.IndexName], "A"));
                    Assert.Equal(1, indexErrors.Length);
                    Assert.Contains("String compilation has been disabled in engine options. You can configure it by modifying the configuration option: 'Indexing.AllowStringCompilation'",
                        indexErrors[0].Errors.First().Error);
                }
            }
        }

        private class JavaScriptIndexWithIdMethod : AbstractJavaScriptIndexCreationTask
        {
            public class Result
            {
                public string Id { get; set; }
                public string Name { get; set; }
                public int Fax { get; set; }
            }

            public override string IndexName => "JavaScriptIndexWithIdMethod";

            public override IndexDefinition CreateIndexDefinition()
            {
                var fieldOptions = new IndexFieldOptions { Storage = FieldStorage.Yes };

                return new IndexDefinition
                {
                    Maps =
                    {
                        @"map(""Companies"", (company) => {
                            if (company.Fax < 10) {
                                return {
                                    Id: id(company),
                                    Name: company.Name,
                                    Fax: company.Fax
                                };
                            }
                        })"
                    },
                    Fields =
                    {
                        { nameof(Result.Id), fieldOptions },
                    }
                };
            }
        }

        private class JavaScriptIndexWithGetMetadataMethod : AbstractJavaScriptIndexCreationTask
        {
            public class Result
            {
                public string Collection { get; set; }
                public string ChangeVector { get; set; }
                public string Id { get; set; }
                public DateTime LastModified { get; set; }
            }

            public override string IndexName => "JavaScriptIndexWith_getMetadataMethod";

            public override IndexDefinition CreateIndexDefinition()
            {
                var fieldOptions = new IndexFieldOptions { Storage = FieldStorage.Yes };

                return new IndexDefinition
                {
                    Maps =
                    {
                        @"map(""Companies"", (company) => {
                            if (company.Fax < 10) {
                                var metadata = getMetadata(company)
                                return {
                                    Collection:     metadata[""@collection""],
                                    ChangeVector:   metadata[""@change-vector""],
                                    Id:             metadata[""@id""],
                                    LastModified:   metadata[""@last-modified""]
                                };
                            }
                        })"
                    },
                    Fields =
                    {
                        { nameof(Result.Collection), fieldOptions },
                        { nameof(Result.ChangeVector), fieldOptions },
                        { nameof(Result.Id), fieldOptions },
                        { nameof(Result.LastModified), fieldOptions },
                    }
                };
            }
        }

        private class MyJSIndex : AbstractJavaScriptIndexCreationTask
        {
            public override string IndexName => "MyJSIndex";

            public class CustomProjection
            {
                public string NewName { get; set; }
            }

            public MyJSIndex()
            {
            }

            public MyJSIndex(bool allowStringCompilation = false)
            {
                Maps = new HashSet<string>()
                {
                    @"
map('Companies', (company) => {
    const script = 'return ""Hello World"";';
    const dynoFunc = new Function(""doc"", script);
    return {
        NewName: dynoFunc(company)
    };
})"
                };

                Configuration = new IndexConfiguration
                {
                    {
                        RavenConfiguration.GetKey(x => x.Indexing.AllowStringCompilation), allowStringCompilation.ToString()
                    }
                };

                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    {
                        nameof(CustomProjection.NewName), new IndexFieldOptions
                        {
                            Storage = FieldStorage.Yes
                        }
                    }
                };
            }
        }
    }
}
