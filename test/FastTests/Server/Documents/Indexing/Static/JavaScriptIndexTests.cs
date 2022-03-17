using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

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

        [Fact]
        public async Task CanUseIdMethodInJavascriptIndex()
        {
            using (var store = GetDocumentStore())
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
                    }

                    await s.SaveChangesAsync();
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

        [Fact]
        public async Task CanUseGetMetadataMethodInJavascriptIndex()
        {
            using (var store = GetDocumentStore())
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

        public class JavaScriptIndexWithIdMethod : AbstractJavaScriptIndexCreationTask
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

        public class JavaScriptIndexWithGetMetadataMethod : AbstractJavaScriptIndexCreationTask
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
    }
}
