using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Client.Queries
{
    public class FullTextSearchOnTags : RavenTestBase
    {
        public FullTextSearchOnTags(ITestOutputHelper output) : base(output)
        {
        }

        private class Image
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public ICollection<string> Users { get; set; }
            public ICollection<string> Tags { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanSearchUsingPhrase(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Image
                    {
                        Tags = new[] { "cats", "animal", "feline" }
                    });
                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Tags }" },
                    Name = "test"
                }}));

                using (var session = store.OpenSession())
                {
                    var images = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats")
                        .ToList();
                    Assert.NotEmpty(images);
                }
            }
        }

        [Theory]
        [RavenData]
        public void CanSearchUsingPhraseAndOrderBy(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Image
                    {
                        Name = "B",
                        Tags = new[] { "cats", "animal", "feline" }
                    });
                    session.Store(new Image
                    {
                        Name = "A",
                        Tags = new[] { "cats", "animal", "feline" }
                    });
                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Tags,doc.Name }" },
                    Name = "test"
                }}));

                using (var session = store.OpenSession())
                {
                    var images = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.Name)
                        .Search(x => x.Tags, "i love cats")
                        .ToList();
                    Assert.NotEmpty(images);

                    Assert.Equal("images/2-A", images[0].Id);
                    Assert.Equal("images/1-A", images[1].Id);
                }
            }
        }

        [Theory]
        [RavenData]
        public void CanSearchUsingPhrase_MultipleSearches(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Image
                    {
                        Tags = new[] { "cats", "animal", "feline" }
                    });

                    session.Store(new Image
                    {
                        Tags = new[] { "dogs", "animal", "canine" }
                    });
                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Tags }" },
                    Name = "test"
                }}));

                using (var session = store.OpenSession())
                {
                    var images = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats")
                        .Search(x => x.Tags, "canine love")
                        .ToList();
                    Assert.Equal(2, images.Count);
                }
            }
        }

        [Theory]
        [RavenData]
        public void StandardSearchWillProduceExpectedResult(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats")
                        .Where(x => x.Name == "User");

                    var query = RavenTestHelper.GetIndexQuery(ravenQueryable);

                    Assert.Equal("from index 'test' where search(Tags, $p0) and (Name = $p1)", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("User", query.QueryParameters["p1"]);
                }
            }
        }



        [Theory]
        [RavenData]
        public void SearchCanUseAnd2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "User")
                        .Search(x => x.Tags, "i love cats", options: SearchOptions.And);

                    var query = RavenTestHelper.GetIndexQuery(ravenQueryable);

                    Assert.Equal("from index 'test' where Name = $p0 and search(Tags, $p1)", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p1"]);
                    Assert.Equal("User", query.QueryParameters["p0"]);
                }
            }
        }

        [Theory]
        [RavenData]
        public void SearchCanUseAnd(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats", options: SearchOptions.And)
                        .Where(x => x.Name == "User");

                    var query = RavenTestHelper.GetIndexQuery(ravenQueryable);

                    Assert.Equal("from index 'test' where search(Tags, $p0) and (Name = $p1)", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("User", query.QueryParameters["p1"]);
                }
            }
        }

        [Theory]
        [RavenData]
        public void SearchCanUseOr(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats", options: SearchOptions.Or)
                        .Where(x => x.Name == "User");

                    var query = RavenTestHelper.GetIndexQuery(ravenQueryable);

                    Assert.Equal("from index 'test' where search(Tags, $p0) or Name = $p1", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("User", query.QueryParameters["p1"]);
                }
            }
        }

        [Theory]
        [RavenData]
        public void SearchWillUseGuessByDefault(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats")
                        .Search(x => x.Users, "i love cats")
                        .Where(x => x.Name == "User");

                    var query = RavenTestHelper.GetIndexQuery(ravenQueryable);

                    Assert.Equal("from index 'test' where (search(Tags, $p0) or search(Users, $p1)) and (Name = $p2)", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("i love cats", query.QueryParameters["p1"]);
                    Assert.Equal("User", query.QueryParameters["p2"]);
                }
            }
        }


        [Theory]
        [RavenData]
        public void ActuallySearchWithAndAndNot(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Image
                    {
                        Tags = new[] { "cats" },
                        Name = "User"
                    });

                    session.Store(new Image
                    {
                        Tags = new[] { "dogs" },
                        Name = "User"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats", options: SearchOptions.And | SearchOptions.Not)
                        .Where(x => x.Name == "User");

                    var query = RavenTestHelper.GetIndexQuery(ravenQueryable);

                    Assert.Equal("from 'Images' where (exists(Tags) and not search(Tags, $p0)) and (Name = $p1)", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("User", query.QueryParameters["p1"]);

                    Assert.Equal(1, ravenQueryable.Count());
                }
            }
        }

        [Theory]
        [RavenData]
        public void SearchCanUseNot(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats", options: SearchOptions.Not)
                        .Where(x => x.Name == "User");

                    var query = RavenTestHelper.GetIndexQuery(ravenQueryable);

                    Assert.Equal("from index 'test' where (exists(Tags) and not search(Tags, $p0)) or Name = $p1", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("User", query.QueryParameters["p1"]);
                }
            }
        }

        [Theory]
        [RavenData]
        public void SearchCanUseNotAndAnd(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats", options: SearchOptions.Not | SearchOptions.And)
                        .Where(x => x.Name == "User");

                    var query = RavenTestHelper.GetIndexQuery(ravenQueryable);

                    Assert.Equal("from index 'test' where (exists(Tags) and not search(Tags, $p0)) and (Name = $p1)", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("User", query.QueryParameters["p1"]);
                }
            }
        }

        [Theory]
        [RavenData]
        public void BoostingSearches(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Tags }" },
                    Name = "test"
                }}));

                using (var session = store.OpenSession())
                {
                    session.Store(new Image
                    {
                        Tags = new[] { "cats", "animal", "feline" }
                    });

                    session.Store(new Image
                    {
                        Tags = new[] { "dogs", "animal", "canine" }
                    });
                    session.Store(new Image
                    {
                        Tags = new[] { "bugs", "resolving", "tricky" }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats", boost: 3)
                        .Search(x => x.Tags, "i love bugs", boost: 20)
                        .Search(x => x.Tags, "canine love", boost: 13);

                    var query = RavenTestHelper.GetIndexQuery(ravenQueryable);

                    Assert.Equal("from index 'test' where (boost(search(Tags, $p0), 3) or boost(search(Tags, $p1), 20) or boost(search(Tags, $p2), 13))", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("i love bugs", query.QueryParameters["p1"]);
                    Assert.Equal("canine love", query.QueryParameters["p2"]);

                    var images = ravenQueryable.ToList();

                    Assert.Equal(3, images.Count);
                    Assert.Equal("images/2-A", images[1].Id);
                    Assert.Equal("images/1-A", images[2].Id);
                    Assert.Equal("images/3-A", images[0].Id);
                }
            }
        }

        [Theory]
        [RavenData]
        public void MultipleSearches(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Image
                    {
                        Tags = new[] { "cats", "animal", "feline" },
                        Users = new[] { "oren", "ayende" }
                    });
                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Tags, doc.Users }" },
                    Name = "test"
                }));

                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats")
                        .Search(x => x.Users, "oren");

                    var query = RavenTestHelper.GetIndexQuery(ravenQueryable);

                    Assert.Equal("from index 'test' where (search(Tags, $p0) or search(Users, $p1))", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("oren", query.QueryParameters["p1"]);
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void UsingSuggest(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Image
                    {
                        Tags = new[] { "cats", "animal", "feline" },
                        Users = new[] { "oren", "ayende" }
                    });
                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Tags, doc.Users }" },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        { "Tags", new IndexFieldOptions { Indexing = FieldIndexing.Search, Suggestions = true} }
                    },
                    Name = "test"
                }}));

                using (var session = store.OpenSession())
                {
                    session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    var query = session.Query<Image>("test")
                        .SuggestUsing(f => f.ByField(x => x.Tags, "animal lover"))
                        .Execute();

                    Assert.NotEmpty(query["Tags"].Suggestions);
                    Assert.Equal("animal", query["Tags"].Suggestions[0]);
                }
            }
        }

        [Theory]
        [RavenData]
        public void Can_search_inner_words(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Image { Id = "1", Name = "Great Photo buddy" });
                    session.Store(new Image { Id = "2", Name = "Nice Photo of the sky" });
                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Name }" },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        { "Name", new IndexFieldOptions { Indexing = FieldIndexing.Search } }
                    },
                    Name = "test"
                }}));

                using (var session = store.OpenSession())
                {
                    var images = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.Name)
                        .Search(x => x.Name, "Photo")
                        .ToList();
                    Assert.NotEmpty(images);
                }
            }
        }

        [Theory]
        [RavenData]
        public void CanSearchFullyAnalyzedTerm(Options options)
        {
            using (var store = GetDocumentStore(options))
            {

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Name }" },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        { "Name", new IndexFieldOptions { Indexing = FieldIndexing.Search } }
                    },
                    Name = "test"
                }}));

                using (var session = store.OpenSession())
                {
                    session.Query<Image>("test")
                        .Search(x => x.Name, "AND")
                        .ToList();
                }
                //If this doesn't throw the test pass
            }
        }

        [Theory]
        [RavenData]
        public void Can_search_inner_words_with_extra_condition(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Image { Id = "1", Name = "Great Photo buddy" });
                    session.Store(new Image { Id = "2", Name = "Nice Photo of the sky" });
                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Name }" },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        { "Name", new IndexFieldOptions { Indexing = FieldIndexing.Search } }
                    },
                    Name = "test"
                }}));

                using (var session = store.OpenSession())
                {
                    var images = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.Name)
                        .Search(x => x.Name, "Photo", options: SearchOptions.And)
                        .Where(x => x.Id == "1")
                        .ToList();

                    Assert.NotEmpty(images);
                    Assert.True(images.Count == 1);
                }
            }
        }

        [Theory]
        [RavenData]
        public void Can_have_special_characters_in_search_text(Options options)
        {
            const string specialCharacters = "+-!(){}:[]^\"~*";
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    foreach (var specialCharacter in specialCharacters)
                    {
                        var qry = session.Query<Image>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Search(x => x.Name, specialCharacter.ToString());

                        var query = RavenTestHelper.GetIndexQuery(qry);

                        Assert.Equal("from 'Images' where search(Name, $p0)", query.Query);
                        Assert.Equal(specialCharacter.ToString(), query.QueryParameters["p0"]);

                        qry.ToList();
                    }
                }
            }
        }

        [Theory]
        [RavenData]
        public void Can_have_special_characters_in_search_text_string(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var qry = session.Query<Image>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Name, "He said: hello there");

                    var query = RavenTestHelper.GetIndexQuery(qry);

                    Assert.Equal("from 'Images' where search(Name, $p0)", query.Query);
                    Assert.Equal("He said: hello there", query.QueryParameters["p0"]);

                    qry.ToList();
                }
            }
        }

        [Theory]
        [RavenData]
        public void Can_search_on_array_of_strings(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Image { Id = "1", Name = "Great Photo buddy" });
                    session.Store(new Image { Id = "2", Name = "Nice Photo of the sky" });
                    session.Store(new Image { Id = "3", Name = "Amazing Photo of flying raven" });
                    session.Store(new Image { Id = "4", Name = "Stunning photo of hibernating rhino" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var imagesQuery = session.Query<Image>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Name, new [] { "buddy", "sky", "raven", "rhino" });

                    var images = imagesQuery.ToList();
                    Assert.Equal(4, images.Count);

                    var query = RavenTestHelper.GetIndexQuery(imagesQuery);
                    Assert.Equal("from 'Images' where search(Name, $p0)", query.Query);
                    Assert.True(query.QueryParameters.TryGetValue("p0", out var searchTerms));
                    Assert.Equal("buddy sky raven rhino", searchTerms);
                }
            }
        }
    }
}
