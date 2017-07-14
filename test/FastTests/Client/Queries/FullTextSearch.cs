using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Xunit;

namespace FastTests.Client.Queries
{
    public class FullTextSearchOnTags : RavenTestBase
    {
        private class Image
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public ICollection<string> Users { get; set; }
            public ICollection<string> Tags { get; set; }
        }

        [Fact]
        public void CanSearchUsingPhrase()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Image
                    {
                        Tags = new[] { "cats", "animal", "feline" }
                    });
                    session.SaveChanges();
                }

                store.Admin.Send(new PutIndexesOperation(new[] {new IndexDefinition
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

        [Fact]
        public void CanSearchUsingPhraseAndOrderBy()
        {
            using (var store = GetDocumentStore())
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

                store.Admin.Send(new PutIndexesOperation(new[] {new IndexDefinition
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

        [Fact]
        public void CanSearchUsingPhrase_MultipleSearches()
        {
            using (var store = GetDocumentStore())
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

                store.Admin.Send(new PutIndexesOperation(new[] { new IndexDefinition
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

        [Fact]
        public void StandardSearchWillProduceExpectedResult()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats")
                        .Where(x => x.Name == "User");

                    var query = GetIndexQuery(ravenQueryable);

                    Assert.Equal("FROM INDEX 'test' WHERE search(Tags, :p0) AND (Name = :p1)", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("User", query.QueryParameters["p1"]);
                }
            }
        }



        [Fact]
        public void SearchCanUseAnd2()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "User")
                        .Search(x => x.Tags, "i love cats", options: SearchOptions.And);

                    var query = GetIndexQuery(ravenQueryable);

                    Assert.Equal("FROM INDEX 'test' WHERE Name = :p0 AND search(Tags, :p1)", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p1"]);
                    Assert.Equal("User", query.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void SearchCanUseAnd()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats", options: SearchOptions.And)
                        .Where(x => x.Name == "User");

                    var query = GetIndexQuery(ravenQueryable);

                    Assert.Equal("FROM INDEX 'test' WHERE search(Tags, :p0) AND (Name = :p1)", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("User", query.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void SearchCanUseOr()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats", options: SearchOptions.Or)
                        .Where(x => x.Name == "User");

                    var query = GetIndexQuery(ravenQueryable);

                    Assert.Equal("FROM INDEX 'test' WHERE search(Tags, :p0) OR Name = :p1", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("User", query.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void SearchWillUseGuessByDefault()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats")
                        .Search(x => x.Users, "i love cats")
                        .Where(x => x.Name == "User");

                    var query = GetIndexQuery(ravenQueryable);

                    Assert.Equal("FROM INDEX 'test' WHERE (search(Tags, :p0) OR search(Users, :p1)) AND (Name = :p2)", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("i love cats", query.QueryParameters["p1"]);
                    Assert.Equal("User", query.QueryParameters["p2"]);
                }
            }
        }


        [Fact]
        public void ActuallySearchWithAndAndNot()
        {
            using (var store = GetDocumentStore())
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

                    var query = GetIndexQuery(ravenQueryable);

                    Assert.Equal("FROM Images WHERE (exists(Tags) AND NOT search(Tags, :p0)) AND (Name = :p1)", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("User", query.QueryParameters["p1"]);

                    Assert.Equal(1, ravenQueryable.Count());
                }
            }
        }

        [Fact]
        public void SearchCanUseNot()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats", options: SearchOptions.Not)
                        .Where(x => x.Name == "User");

                    var query = GetIndexQuery(ravenQueryable);

                    Assert.Equal("FROM INDEX 'test' WHERE (exists(Tags) AND NOT search(Tags, :p0)) OR Name = :p1", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("User", query.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void SearchCanUseNotAndAnd()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Tags, "i love cats", options: SearchOptions.Not | SearchOptions.And)
                        .Where(x => x.Name == "User");

                    var query = GetIndexQuery(ravenQueryable);

                    Assert.Equal("FROM INDEX 'test' WHERE (exists(Tags) AND NOT search(Tags, :p0)) AND (Name = :p1)", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("User", query.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void BoostingSearches()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexesOperation(new[] {new IndexDefinition
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

                    var query = GetIndexQuery(ravenQueryable);

                    Assert.Equal("FROM INDEX 'test' WHERE (boost(search(Tags, :p0), 3) OR boost(search(Tags, :p1), 20) OR boost(search(Tags, :p2), 13))", query.Query);
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

        [Fact]
        public void MultipleSearches()
        {
            using (var store = GetDocumentStore())
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

                store.Admin.Send(new PutIndexesOperation(new IndexDefinition
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

                    var query = GetIndexQuery(ravenQueryable);

                    Assert.Equal("FROM INDEX 'test' WHERE (search(Tags, :p0) OR search(Users, :p1))", query.Query);
                    Assert.Equal("i love cats", query.QueryParameters["p0"]);
                    Assert.Equal("oren", query.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void UsingSuggest()
        {
            using (var store = GetDocumentStore())
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

                store.Admin.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Tags, doc.Users }" },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        { "Tags", new IndexFieldOptions { Indexing = FieldIndexing.Analyzed, Suggestions = true} }
                    },
                    Name = "test"
                }}));

                using (var session = store.OpenSession())
                {
                    session.Query<Image>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    var query = session.Query<Image>("test")
                        .Search(x => x.Tags, "animal lover")
                        .Suggest();

                    Assert.NotEmpty(query.Suggestions);
                    Assert.Equal("animal", query.Suggestions[0]);
                }
            }
        }

        [Fact]
        public void Can_search_inner_words()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Image { Id = "1", Name = "Great Photo buddy" });
                    session.Store(new Image { Id = "2", Name = "Nice Photo of the sky" });
                    session.SaveChanges();
                }

                store.Admin.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Name }" },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        { "Name", new IndexFieldOptions { Indexing = FieldIndexing.Analyzed } }
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

        [Fact]
        public void CanSearchFullyAnalyzedTerm()
        {
            using (var store = GetDocumentStore())
            {

                store.Admin.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Name }" },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        { "Name", new IndexFieldOptions { Indexing = FieldIndexing.Analyzed } }
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

        [Fact]
        public void Can_search_inner_words_with_extra_condition()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Image { Id = "1", Name = "Great Photo buddy" });
                    session.Store(new Image { Id = "2", Name = "Nice Photo of the sky" });
                    session.SaveChanges();
                }

                store.Admin.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Name }" },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        { "Name", new IndexFieldOptions { Indexing = FieldIndexing.Analyzed } }
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

        [Fact]
        public void Can_have_special_characters_in_search_text()
        {
            const string specialCharacters = "+-!(){}:[]^\"~*";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    foreach (var specialCharacter in specialCharacters)
                    {
                        var qry = session.Query<Image>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Search(x => x.Name, specialCharacter.ToString());

                        var query = GetIndexQuery(qry);

                        Assert.Equal("FROM Images WHERE search(Name, :p0)", query.Query);
                        Assert.Equal(string.Format("\\{0}", specialCharacter), query.QueryParameters["p0"]);

                        qry.ToList();
                    }
                }
            }
        }

        [Fact]
        public void Can_have_special_characters_in_search_text_string()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var qry = session.Query<Image>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Name, "He said: hello there");

                    var query = GetIndexQuery(qry);

                    Assert.Equal("FROM Images WHERE search(Name, :p0)", query.Query);
                    Assert.Equal("He said\\: hello there", query.QueryParameters["p0"]);

                    qry.ToList();
                }
            }
        }

        private static IndexQuery GetIndexQuery(IQueryable<Image> queryable)
        {
            var inspector = (IRavenQueryInspector)queryable;
            return inspector.GetIndexQuery(isAsync: false);
        }
    }
}