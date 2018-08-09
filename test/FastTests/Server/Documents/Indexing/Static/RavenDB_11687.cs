using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class RavenDB_11687 : RavenTestBase
    {
        [Fact]
        public void CanIndexDictionaryDirectly()
        {
            using (var store = GetDocumentStore())
            {
                new IndexReturningDictionary_MethodSyntax().Execute(store);
                new IndexReturningDictionary_QuerySyntax().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "arek",
                        Age = 32,
                    });

                    session.Store(new User()
                    {
                        Name = "joe",
                        Age = 33
                    });

                    session.SaveChanges();

                    var users = session.Query<User, IndexReturningDictionary_MethodSyntax>().Customize(x => x.WaitForNonStaleResults()).ToList();
                    Assert.Equal(2, users.Count);

                    users = session.Query<User, IndexReturningDictionary_MethodSyntax>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age == 32).ToList();
                    Assert.Equal(1, users.Count);
                    Assert.Equal("arek", users[0].Name);

                    users = session.Query<User, IndexReturningDictionary_QuerySyntax>().Customize(x => x.WaitForNonStaleResults()).ToList();
                    Assert.Equal(2, users.Count);

                    users = session.Query<User, IndexReturningDictionary_QuerySyntax>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age == 32).ToList();
                    Assert.Equal(1, users.Count);
                    Assert.Equal("arek", users[0].Name);
                }
            }
        }

        [Fact]
        public void CanMapReduceIndexDictionaryDirectly()
        {
            using (var store = GetDocumentStore())
            {
                new MapReduceIndexReturningDictionary_MethodSyntax().Execute(store);
                new MapReduceIndexReturningDictionary_QuerySyntax().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "arek",
                        Age = 32
                    });

                    session.Store(new User()
                    {
                        Name = "joe",
                        Age = 32
                    });

                    session.SaveChanges();

                    var results = session.Query<MapReduceIndexReturningDictionary_MethodSyntax.Result, MapReduceIndexReturningDictionary_MethodSyntax>().Customize(x => x.WaitForNonStaleResults()).ToList();
                    Assert.Equal(1, results.Count);

                    Assert.Equal(2, results[0].Count);
                    Assert.Equal(32, results[0].Age);

                    results = session.Query<MapReduceIndexReturningDictionary_MethodSyntax.Result, MapReduceIndexReturningDictionary_QuerySyntax>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    WaitForUserToContinueTheTest(store);
                    Assert.Equal(1, results.Count);

                    Assert.Equal(2, results[0].Count);
                    Assert.Equal(32, results[0].Age);
                }
            }
        }

        [Fact]
        public void CanIndexDictionaryWithComplexObjectsDirectly()
        {
            using (var store = GetDocumentStore())
            {
                new IndexReturningDictionaryWithComplexObjects_MethodSyntax().Execute(store);
                new IndexReturningDictionaryWithComplexObjects_QuerySyntax().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new PersonWithAddress()
                    {
                        Name = "joe",
                        Address = new Address()
                        {
                            City = "NY",
                            Country = "USA",
                            ZipCode = 1
                        }
                    });

                    session.Store(new PersonWithAddress()
                    {
                        Name = "doe",
                        Address = new Address()
                        {
                            City = "LA",
                            Country = "USA",
                            ZipCode = 2
                        }
                    });

                    session.SaveChanges();

                    // IndexReturningDictionaryWithComplexObjects_MethodSyntax

                    var people = session.Query<PersonWithAddress, IndexReturningDictionaryWithComplexObjects_MethodSyntax>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Equal(2, people.Count);

                    people = session.Query<PersonWithAddress, IndexReturningDictionaryWithComplexObjects_MethodSyntax>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Address.ZipCode == 1).ToList();

                    Assert.Equal(1, people.Count);
                    Assert.Equal("joe", people[0].Name);

                    people = session.Query<PersonWithAddress, IndexReturningDictionaryWithComplexObjects_MethodSyntax>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Address, "LA").ToList();

                    Assert.Equal(1, people.Count);
                    Assert.Equal("doe", people[0].Name);

                    people = session.Query<IndexReturningDictionaryWithComplexObjects_MethodSyntax.Result, IndexReturningDictionaryWithComplexObjects_MethodSyntax>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.DictField, "joe")
                        .OfType<PersonWithAddress>()
                        .ToList();

                    Assert.Equal(1, people.Count);
                    Assert.Equal("joe", people[0].Name);

                    // IndexReturningDictionaryWithComplexObjects_QuerySyntax

                    people = session.Query<PersonWithAddress, IndexReturningDictionaryWithComplexObjects_QuerySyntax>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Equal(2, people.Count);

                    people = session.Query<PersonWithAddress, IndexReturningDictionaryWithComplexObjects_QuerySyntax>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Address.ZipCode == 1).ToList();

                    Assert.Equal(1, people.Count);
                    Assert.Equal("joe", people[0].Name);

                    people = session.Query<PersonWithAddress, IndexReturningDictionaryWithComplexObjects_QuerySyntax>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Address, "LA").ToList();

                    Assert.Equal(1, people.Count);
                    Assert.Equal("doe", people[0].Name);

                    people = session.Query<IndexReturningDictionaryWithComplexObjects_MethodSyntax.Result, IndexReturningDictionaryWithComplexObjects_QuerySyntax>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.DictField, "joe")
                        .OfType<PersonWithAddress>()
                        .ToList();

                    Assert.Equal(1, people.Count);
                    Assert.Equal("joe", people[0].Name);
                }
            }
        }

        [Fact]
        public void CanMapReduceIndexDictionaryWithComplexObjectsDirectly()
        {
            using (var store = GetDocumentStore())
            {
                new MapReduceIndexReturningDictionaryWithComplexObjects_MethodSyntax().Execute(store);
                new MapReduceIndexReturningDictionaryWithComplexObjects_QuerySyntax().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new PersonWithAddress()
                    {
                        Name = "joe",
                        Address = new Address()
                        {
                            City = "NY",
                            Country = "USA",
                            ZipCode = 1
                        }
                    });

                    session.Store(new PersonWithAddress()
                    {
                        Name = "doe",
                        Address = new Address()
                        {
                            City = "NY",
                            Country = "USA",
                            ZipCode = 1
                        }
                    });

                    session.SaveChanges();

                    // MapReduceIndexReturningDictionaryWithComplexObjects_MethodSyntax

                    var results = session.Query<MapReduceIndexReturningDictionaryWithComplexObjects_MethodSyntax.Result, MapReduceIndexReturningDictionaryWithComplexObjects_MethodSyntax>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(1, results[0].Names.Count);
                    Assert.NotNull(results[0].Names["Name"]);


                    results = session.Query<MapReduceIndexReturningDictionaryWithComplexObjects_MethodSyntax.Result, MapReduceIndexReturningDictionaryWithComplexObjects_MethodSyntax>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Address, "USA")
                        .ToList();

                    Assert.Equal(1, results.Count);

                    // MapReduceIndexReturningDictionaryWithComplexObjects_QuerySyntax

                    results = session.Query<MapReduceIndexReturningDictionaryWithComplexObjects_MethodSyntax.Result, MapReduceIndexReturningDictionaryWithComplexObjects_QuerySyntax>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(1, results[0].Names.Count);
                    Assert.NotNull(results[0].Names["Name"]);


                    results = session.Query<MapReduceIndexReturningDictionaryWithComplexObjects_MethodSyntax.Result, MapReduceIndexReturningDictionaryWithComplexObjects_QuerySyntax>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Search(x => x.Address, "USA")
                        .ToList();

                    Assert.Equal(1, results.Count);
                }
            }
        }

        [Fact]
        public void CanIndexUsingDictionaryOutputPreceededBySelectWithAnonnymus()
        {
            using (var store = GetDocumentStore())
            {
                new MixedSelectWithAnonymusAndDictionary().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Address()
                    {
                        City = "NY",
                        Country = "USA"
                    }, "addresses/1");

                    session.Store(new Person()
                    {
                        AddressId = "addresses/1",
                        Name = "joe"
                    });

                    session.SaveChanges();

                    var persons = session.Query<Person, MixedSelectWithAnonymusAndDictionary>().Customize(x => x.WaitForNonStaleResults()).ToList();
                }
            }
        }

        private class IndexReturningDictionary_MethodSyntax : AbstractIndexCreationTask<User>
        {
            public IndexReturningDictionary_MethodSyntax()
            {
                Map = users => users.Select(x => new Dictionary<string, object>()
                {
                    {"Age", x.Age},
                    {"Name", x.Name}
                });
            }
        }

        private class MapReduceIndexReturningDictionary_MethodSyntax : AbstractIndexCreationTask<User>
        {
            public class Result
            {
                public int Age { get; set; }
                public int Count { get; set; }
            }

            public MapReduceIndexReturningDictionary_MethodSyntax()
            {
                Map = users => users.Select(x => new Dictionary<string, object>()
                {
                    {"Age", x.Age},
                    {"Count", 1}
                });

                Reduce = results => results.GroupBy(x => x.Age).Select(x => new Dictionary<string, object>()
                {
                    {"Age", x.Key},
                    {"Count", x.Sum(y => y.Count)}
                });
            }
        }

        private class IndexReturningDictionary_QuerySyntax : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = new HashSet<string>
                    {
                        @"from user in docs.Users select new Dictionary <string, object >() { {""Age"", user.Age}, {""Name"", user.Name} }"
                    }
                };
            }
        }

        private class MapReduceIndexReturningDictionary_QuerySyntax : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = new HashSet<string>
                    {
                        @"from user in docs.Users select new Dictionary<string, object>() { {""Age"", user.Age}, {""Count"", 1} }"
                    },
                    Reduce = @"from result in results group result by result.Age into g select new Dictionary<string, object>() { {""Age"", g.Key}, {""Count"", g.Sum(x => x.Count)} }"
                };
            }
        }

        private class IndexReturningDictionaryWithComplexObjects_MethodSyntax : AbstractIndexCreationTask<PersonWithAddress>
        {
            public class Result
            {
                public int Address_ZipCode { get; set; }
                public Address Address { get; set; }
                public Dictionary<string, object> DictField { get; set; }
            }

            public IndexReturningDictionaryWithComplexObjects_MethodSyntax()
            {
                Map = users => users.Select(x => new Dictionary<string, object>()
                {
                    {"Address_ZipCode", x.Address.ZipCode},
                    {nameof(PersonWithAddress.Address), new Address
                        {
                            City = x.Address.City,
                            Country = x.Address.Country
                        }
                    },
                    {nameof(Result.DictField), new Dictionary<string, object>()
                        {
                            {"Name", x.Name}
                        }
                    }
                });

                Index(nameof(Result.Address), FieldIndexing.Search);
                Index(nameof(Result.DictField), FieldIndexing.Search);
            }
        }

        private class IndexReturningDictionaryWithComplexObjects_QuerySyntax : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = new HashSet<string>
                    {
                        @"from person in docs.PersonWithAddresses select new Dictionary<string, object>() 
                            { 
                                {""Address_ZipCode"", person.Address.ZipCode},
                                {""Address"", new
                                    {
                                        City = person.Address.City,
                                        Country = person.Address.Country
                                    }
                                },
                                {""DictField"", new Dictionary<string, object>()
                                    {
                                        {""Name"", person.Name}
                                    }
                                },
                            }"
                    },
                    Fields =
                    {
                        {"Address", new IndexFieldOptions{ Indexing = FieldIndexing.Search }},
                        {"DictField", new IndexFieldOptions{ Indexing = FieldIndexing.Search }}
                    }
                };
            }
        }

        private class MapReduceIndexReturningDictionaryWithComplexObjects_MethodSyntax : AbstractIndexCreationTask<PersonWithAddress, MapReduceIndexReturningDictionaryWithComplexObjects_MethodSyntax.Result>
        {
            public class Result
            {
                public Address Address { get; set; }
                public Dictionary<string, object> Names { get; set; }
            }

            public MapReduceIndexReturningDictionaryWithComplexObjects_MethodSyntax()
            {
                Map = users => users.Select(x => new Dictionary<string, object>()
                {
                    {nameof(Result.Address), new Address
                        {
                            City = x.Address.City,
                            Country = x.Address.Country
                        }
                    },
                    {nameof(Result.Names), new Dictionary<string, object>()
                        {
                            {"Name", x.Name}
                        }
                    }
                });

                Reduce = results => results.GroupBy(x => x.Address)
                    .Select(x => new Dictionary<string, object>
                    {
                        {nameof(Result.Address), new Address { City = x.Key.City, Country = x.Key.Country }},
                        {nameof(Result.Names), new Dictionary<string, object>()
                        {
                            {"Name", x.Select(z => z.Names["Name"]).First()}
                        }}
                    });

                Index(nameof(Result.Address), FieldIndexing.Search);
                Index(nameof(Result.Names), FieldIndexing.Search);
            }
        }

        private class MapReduceIndexReturningDictionaryWithComplexObjects_QuerySyntax : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = new HashSet<string>
                    {
                        @"from person in docs.PersonWithAddresses select new Dictionary<string, object>() 
                        { 
                            {""Address"", new
                                {
                                    City = person.Address.City,
                                    Country = person.Address.Country
                                }
                            },
                            {""Names"", new Dictionary<string, object>()
                                {
                                    {""Name"", person.Name}
                                }
                            }
                        }"
                    },
                    Reduce = @"from result in results group result by result.Address into g select new Dictionary<string, object>() 
                        {
                            {""Address"", new { City = g.Key.City, Country = g.Key.Country }},
                            {""Names"", new Dictionary<string, object>()
                                {
                                    {""Name"", g.Select(z => z.Names[""Name""]).First()}
                                }
                            }
                        }",

                    Fields =
                    {
                        {"Address", new IndexFieldOptions{ Indexing = FieldIndexing.Search }},
                        {"Names", new IndexFieldOptions{ Indexing = FieldIndexing.Search }}
                    }
                };
            }
        }

        public class MixedSelectWithAnonymusAndDictionary : AbstractIndexCreationTask<Person>
        {
            public MixedSelectWithAnonymusAndDictionary()
            {
                Map = users => users.Select(x => new
                {
                    Name = x.Name,
                    Address = LoadDocument<Address>(x.AddressId)
                }).Select(x => new Dictionary<string, object>()
                {
                    {"Name", x.Name},
                    {"Adddress_City", x.Address.City},
                    {"Adddress_Country", x.Address.Country},
                });
            }
        }
    }
}
