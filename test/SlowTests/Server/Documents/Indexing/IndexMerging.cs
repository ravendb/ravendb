using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Indexes.IndexMerging;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing
{
    public class IndexMerging : RavenTestBase
    {
        public IndexMerging(ITestOutputHelper output) : base(output)
        {
        }

        private class Person
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string AddressId { get; set; }
        }

        private class PersonWithAddress
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public Address Address { get; set; }
        }

        private class Address
        {
            public string Id { get; set; }
            public string Street { get; set; }
            public int ZipCode { get; set; }
        }

        private class User
        {
            public string Name { get; set; }
            public string Email { get; set; }
            public int Age { get; set; }
        }

        private class UsersByName : AbstractIndexCreationTask<User>
        {
            public UsersByName()
            {
                Map = usersCollection => from user in usersCollection
                                         select new { user.Name };
                Index(x => x.Name, FieldIndexing.Search);
            }
        }

        private class UsersByAge : AbstractIndexCreationTask<User>
        {
            public UsersByAge()
            {
                Map = users => from u in users
                               select new { u.Age };
            }
        }

        private class UsersByEmail : AbstractIndexCreationTask<User>
        {
            public UsersByEmail()
            {
                Map = users => from user in users
                               select new { user.Email };
            }
        }

        private class Person_ByName_1 : AbstractIndexCreationTask<Person>
        {
            public Person_ByName_1()
            {
                Map = persons => from p in persons
                                 select new
                                 {
                                     Name = p.Name
                                 };
            }
        }

        private class Person_ByName_2 : AbstractIndexCreationTask<Person>
        {
            public Person_ByName_2()
            {
                Map = persons => from p in persons
                                 select new
                                 {
                                     Name = p.Name
                                 };
            }
        }

        private class Person_ByName_3 : AbstractIndexCreationTask<Person>
        {
            public Person_ByName_3()
            {
                Map = persons => from person in persons
                                 select new
                                 {
                                     Name = person.Name
                                 };
            }
        }

        private class Complex_Person_ByName_1 : AbstractIndexCreationTask<PersonWithAddress>
        {
            public Complex_Person_ByName_1()
            {
                Map = persons => from p in persons
                                 select new
                                 {
                                     Street = p.Address.Street
                                 };
            }
        }

        private class Complex_Person_ByName_2 : AbstractIndexCreationTask<PersonWithAddress>
        {
            public Complex_Person_ByName_2()
            {
                Map = persons => from p in persons
                                 select new
                                 {
                                     Street = p.Address.Street
                                 };
            }
        }

        private class Complex_Person_ByName_3 : AbstractIndexCreationTask<PersonWithAddress>
        {
            public Complex_Person_ByName_3()
            {
                Map = persons => from person in persons
                                 select new
                                 {
                                     Street = person.Address.Street
                                 };
            }
        }

        [Fact]
        public void IndexMergeWithField()
        {
            using (var store = GetDocumentStore())
            {
                new UsersByName().Execute(store);
                new UsersByEmail().Execute(store);
                new UsersByAge().Execute(store);

                var index1 = store.Maintenance.Send(new GetIndexOperation("UsersByName"));
                var index2 = store.Maintenance.Send(new GetIndexOperation("UsersByEmail"));
                var index3 = store.Maintenance.Send(new GetIndexOperation("UsersByAge"));

                var dictionary = new Dictionary<string, IndexDefinition>
                {
                    {index1.Name, index1},
                    {index2.Name, index2},
                    {index3.Name, index3}
                };

                var merger = new IndexMerger(dictionary);
                var results = merger.ProposeIndexMergeSuggestions();

                Assert.Equal(1, results.Suggestions.Count);
                var suggestion = results.Suggestions[0];
                var index = suggestion.MergedIndex;

                Assert.Equal(3, suggestion.CanMerge.Count);
                Assert.Equal(FieldIndexing.Search, index.Fields["Name"].Indexing);
                RavenTestHelper.AssertEqualRespectingNewLines(@"docs.Users.Select(doc=>new{Age=doc.Age
,Email=doc.Email
,Name=doc.Name
})", index.Maps.First());
            }
        }

        [Fact]
        public void IndexMergerShouldNotTakeIntoAccountExpressionVariableName()
        {
            var index1 = new Person_ByName_1();
            var index2 = new Person_ByName_2();
            var index3 = new Person_ByName_3();

            var indexDefinition1 = index1.CreateIndexDefinition();
            var indexDefinition2 = index2.CreateIndexDefinition();
            var indexDefinition3 = index3.CreateIndexDefinition();

            var merger = new IndexMerger(
                new Dictionary<string, IndexDefinition>
                {
                    { indexDefinition1.Name, indexDefinition1 },
                    { indexDefinition2.Name, indexDefinition2 }
                });

            var results = merger.ProposeIndexMergeSuggestions();

            Assert.Equal(1, results.Suggestions.Count);
            Assert.Equal(1, results.Suggestions[0].CanDelete.Count);

            merger = new IndexMerger(
                new Dictionary<string, IndexDefinition>
                {
                    { indexDefinition1.Name, indexDefinition1 },
                    { indexDefinition3.Name, indexDefinition3 }
                });

            results = merger.ProposeIndexMergeSuggestions();

            Assert.Equal(1, results.Suggestions.Count);
            Assert.Equal(1, results.Suggestions[0].CanDelete.Count);
        }

        [Fact]
        public void IndexMergerShouldNotTakeIntoAccountExpressionVariableNameForComplexTypes()
        {
            var index1 = new Complex_Person_ByName_1();
            var index2 = new Complex_Person_ByName_2();
            var index3 = new Complex_Person_ByName_3();

            var indexDefinition1 = index1.CreateIndexDefinition();
            var indexDefinition2 = index2.CreateIndexDefinition();
            var indexDefinition3 = index3.CreateIndexDefinition();

            var merger = new IndexMerger(
                new Dictionary<string, IndexDefinition>
                {
                    { indexDefinition1.Name, indexDefinition1 },
                    { indexDefinition2.Name, indexDefinition2 }
                });

            var results = merger.ProposeIndexMergeSuggestions();

            Assert.Equal(1, results.Suggestions.Count);
            Assert.Equal(1, results.Suggestions[0].CanDelete.Count);

            merger = new IndexMerger(
                new Dictionary<string, IndexDefinition>
                {
                    { indexDefinition1.Name, indexDefinition1 },
                    { indexDefinition3.Name, indexDefinition3 }
                });

            results = merger.ProposeIndexMergeSuggestions();

            Assert.Equal(1, results.Suggestions.Count);
            Assert.Equal(1, results.Suggestions[0].CanDelete.Count);
        }

        [Fact]
        public void IndexMergeWithQueryExpressionSyntax()
        {
            using (var store = GetDocumentStore())
            {
                var byName = new IndexDefinition
                {
                    Name = "Users_ByName",
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Type = IndexType.Map
                };
                var byAge = new IndexDefinition
                {
                    Name = "Users_ByAge",
                    Maps = { "from u in docs.Users select new { u.Age }" },
                    Type = IndexType.Map
                };
                var byEmail = new IndexDefinition
                {
                    Name = "Users_ByEmail",
                    Maps = { "from user in docs.Users select new { user.Email }" },
                    Type = IndexType.Map
                };

                store.Maintenance.Send(new PutIndexesOperation(byName, byEmail, byAge));

                var dictionary = new Dictionary<string, IndexDefinition>
                {
                    {byName.Name, byName},
                    {byAge.Name, byAge},
                    {byEmail.Name, byEmail}
                };

                var merger = new IndexMerger(dictionary);
                var results = merger.ProposeIndexMergeSuggestions();

                Assert.Equal(1, results.Suggestions.Count);
                var suggestion = results.Suggestions[0];
                var index = suggestion.MergedIndex;

                Assert.Equal(3, suggestion.CanMerge.Count);
                RavenTestHelper.AssertEqualRespectingNewLines(@"fromdocindocs.Users
selectnew{Age=doc.Age
,Email=doc.Email
,Name=doc.Name
}", index.Maps.First());
            }
        }

        [Fact]
        public void IndexMergerWithQueryExpressionSyntaxShouldNotTakeIntoAccountExpressionVariableName()
        {
            using (var store = GetDocumentStore())
            {
                var index1 = new IndexDefinition
                {
                    Name = "Users_ByName_1",
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Type = IndexType.Map
                };
                var index2 = new IndexDefinition
                {
                    Name = "Users_ByName_2",
                    Maps = { "from u in docs.Users select new { u.Name }" },
                    Type = IndexType.Map
                };

                store.Maintenance.Send(new PutIndexesOperation(index1, index2));

                var dictionary = new Dictionary<string, IndexDefinition>
                {
                    {index1.Name, index1},
                    {index2.Name, index2}
                };

                var merger = new IndexMerger(dictionary);
                var results = merger.ProposeIndexMergeSuggestions();

                Assert.Equal(1, results.Suggestions.Count);
                Assert.Equal(1, results.Suggestions[0].CanDelete.Count);
            }
        }
    }
}
