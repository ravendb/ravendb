using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Indexes.IndexMerging;
using Tests.Infrastructure;
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
                Assert.Equal(@"docs.Users.Select(doc => new { Age = doc.Age, Email = doc.Email, Name = doc.Name })", index.Maps.First());
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
                RavenTestHelper.AssertEqualRespectingNewLines(@"from doc in docs.Users
select new
{
    Age = doc.Age,
    Email = doc.Email,
    Name = doc.Name
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

        [Fact]
        public void CannotMergeWhenIndexContainsWhereClause()
        {
            var index1 = new IndexDefinition
            {
                Name = "Orders/ByShipment/Location",
                Maps = { @"from order in docs.Orders
where order.ShipTo.Location != null
select new
{
    order.Employee,
    order.Company,
    ShipmentLocation = CreateSpatialField(order.ShipTo.Location.Latitude, order.ShipTo.Location.Longitude)
}" },
                Type = IndexType.Map
            };
            var index2 = new IndexDefinition
            {
                Name = "Orders/Totals",
                Maps = { @"from order in docs.Orders
select new
{
    order.Employee,
    order.Company,
    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
}" },
                Type = IndexType.Map
            };

            var results = GetMergeReportOfTwoIndexes(index1, index2);

            Assert.Equal(0, results.Suggestions.Count);
            Assert.Equal("Cannot merge indexes that have a where clause.", results.Unmergables[index1.Name]);
        }

        [Fact]
        public void CanMergeSimpleIndexAndReplaceInnerNamesCorrectly()
        {
            var index1 = new IndexDefinition
            {
                Name = "Product/Search",
                Maps = { @"from p in docs.Products
select new
{
    p.Name,
    p.Category,
    p.Supplier,
    p.PricePerUnit
}" },
                Type = IndexType.Map
            };
            var index2 = new IndexDefinition
            {
                Name = "Products/ByUnitOnStock",
                Maps = { @"from product in docs.Products
select new {
    UnitOnStock = LoadCompareExchangeValue(Id(product))
}" },
                Type = IndexType.Map
            };

            var results = GetMergeReportOfTwoIndexes(index1, index2);

            Assert.Equal(1, results.Suggestions.Count);
            RavenTestHelper.AssertEqualRespectingNewLines(@"from doc in docs.Products
select new
{
    Category = doc.Category,
    Name = doc.Name,
    PricePerUnit = doc.PricePerUnit,
    Supplier = doc.Supplier,
    UnitOnStock = LoadCompareExchangeValue(Id(doc))
}"
                , results.Suggestions.First().MergedIndex.Maps.First());
        }


        [Fact]
        public void ProposeDeleteWhenOneIndexIsSubsetOfAnother()
        {
            var index1 = new IndexDefinition
            {
                Name = "Orders/ByShipment/Location",
                Maps = { @"from order in docs.Orders
select new
{
    order.Employee,
    order.Company,
    ShipmentLocation = CreateSpatialField(order.ShipTo.Location.Latitude, order.ShipTo.Location.Longitude),
    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
}" },
                Type = IndexType.Map
            };
            var index2 = new IndexDefinition
            {
                Name = "Orders/Totals",
                Maps = { @"from order in docs.Orders
select new
{
    order.Employee,
    order.Company,
    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
}" },
                Type = IndexType.Map
            };

            var results = GetMergeReportOfTwoIndexes(index2, index1);
            Assert.Equal(0, results.Suggestions[0].CanMerge.Count);
            Assert.Equal(1, results.Suggestions[0].CanDelete.Count);
            Assert.Equal("Orders/Totals", results.Suggestions[0].CanDelete[0]);
        }


        [Fact]
        public void CanMergeCorrectlyWithDifferentDocumentIdentifiers()
        {
            var index1 = new IndexDefinition
            {
                Name = "Orders/ByShipment/Location",
                Maps = { @"from order in docs.Orders
select new
{
    order.Employee,
    order.Company,
    ShipmentLocation = CreateSpatialField(order.ShipTo.Location.Latitude, order.ShipTo.Location.Longitude)
}" },
                Type = IndexType.Map
            };
            var index2 = new IndexDefinition
            {
                Name = "Orders/Totals",
                Maps = { @"from test in docs.Orders
select new
{
    test.Employee,
    test.Company,
    Total = test.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
}" },
                Type = IndexType.Map
            };

            var results = GetMergeReportOfTwoIndexes(index2, index1);

            Assert.Equal(1, results.Suggestions.Count);
            RavenTestHelper.AssertEqualRespectingNewLines(@"from doc in docs.Orders
select new
{
    Company = doc.Company,
    Employee = doc.Employee,
    ShipmentLocation = CreateSpatialField(doc.ShipTo.Location.Latitude, doc.ShipTo.Location.Longitude),
    Total = doc.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
}", results.Suggestions.First().MergedIndex.Maps.First());
        }

        [Fact]
        public void CanMergeWhenBinaryExpressionIsInsideIndex()
        {
            var index1 = new IndexDefinition
            {
                Name = "Orders/ByShipment/Location",
                Maps = { @"from order in docs.Orders
select new
{
    order.Employee,
    order.Company,
    TotalSum = order.Day.Add(order.A + order.B)
}" },
                Type = IndexType.Map
            };
            var index2 = new IndexDefinition
            {
                Name = "Orders/Totals",
                Maps = { @"from test in docs.Orders
select new
{
    test.Employee,
    test.Company,
    Total = test.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
}" },
                Type = IndexType.Map
            };
            var results = GetMergeReportOfTwoIndexes(index2, index1);

            Assert.Equal(1, results.Suggestions.Count);
            RavenTestHelper.AssertEqualRespectingNewLines(@"from doc in docs.Orders
select new
{
    Company = doc.Company,
    Employee = doc.Employee,
    Total = doc.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount)),
    TotalSum = doc.Day.Add(doc.A + doc.B)
}", results.Suggestions.First().MergedIndex.Maps.First());
        }

        [Fact]
        public void CannotMergeSameIndexesWhenCollectionsDoesntMatch()
        {
            var index1 = new IndexDefinition
            {
                Name = "OrdersA",
                Maps = { @"from order in docs.Orders
select new
{
    order.Employee,
    order.Company,
    TotalSum = order.Day.Add(order.A + order.B)
}" },
                Type = IndexType.Map
            };
            var index2 = new IndexDefinition
            {
                Name = "OrdersB",
                Maps = { @"from order in docs.Returns
select new
{
    order.Employee,
    order.Company,
    TotalSum = order.Day.Add(order.A + order.B)
}" },
                Type = IndexType.Map
            };

            var results = GetMergeReportOfTwoIndexes(index2, index1);

            Assert.Equal(0, results.Suggestions.Count);
        }

        
        [Fact]
        public void CanRewriteDictAccessorAndCoalescingExpression()
        {
            var index1 = new IndexDefinition
            {
                Name = "OrdersA",
                Maps = { @"from order in docs.Orders
select new
{
    Employee2 = order.Employee[""maciej""],
    Company2 = order.Company ?? DateTime.MinValue,
}" },
                Type = IndexType.Map
            };
            var index2 = new IndexDefinition
            {
                Name = "OrdersB",
                Maps = { @"from ord in docs.Orders
select new
{
    ord.Employee,
    ord.Company,
    TotalSum = ord.Day.Add(ord.A + ord.B)
}" },
                Type = IndexType.Map
            };

            var results = GetMergeReportOfTwoIndexes(index2, index1);

            Assert.Equal(1, results.Suggestions.Count);
            RavenTestHelper.AssertEqualRespectingNewLines(@"from doc in docs.Orders
select new
{
    Company = doc.Company,
    Company2 = doc.Company ?? DateTime.MinValue,
    Employee = doc.Employee,
    Employee2 = doc.Employee[""maciej""],
    TotalSum = doc.Day.Add(doc.A + doc.B)
}", results.Suggestions.First().MergedIndex.Maps.First());
        }
        
        [Fact]
        public void AutoIndexesWillNotBeIncludedInOperationOutput()
        {
            using var store = GetDocumentStore();
            {
                using var session = store.OpenSession();
                session.Store(new AutoIndexMockup("Maciej", 1));
                session.SaveChanges();
            }

            {
                using var session = store.OpenSession();
                var createAutoIndexMap = session.Query<AutoIndexMockup>().Where(i => i.Name == "maciej").ToList();
                var createAutoIndexReduce = session.Query<AutoIndexMockup>().GroupBy(i => i.Count).Select(x => new
                {
                    Name = x.Key,
                    Count = x.Count(),
                }).ToList();
            }
            var mapName = "Auto/AutoIndexMockups/ByName";
            var mapIndex = store
                .Maintenance
                .Send(new GetIndexOperation(mapName));
            var reduceName = "Auto/AutoIndexMockupsReducedByCount";
            var mapReduce = store
                .Maintenance
                .Send(new GetIndexOperation(reduceName));
            var dictionary = new Dictionary<string, IndexDefinition>
            {
                {mapName, mapIndex},
                {reduceName, mapReduce}
            };
            var merger = new IndexMerger(dictionary);
            Assert.Equal(0, merger.ProposeIndexMergeSuggestions().Suggestions.Count);
            Assert.Equal(0, merger.ProposeIndexMergeSuggestions().Unmergables.Count);
        }
        
        private class TestIndex : AbstractIndexCreationTask<object, TestIndex.Result>
        {
            public class Result
            {
                public object Doc { get; set; }
            }

            public TestIndex()
            {
                Map = objects => from obj in objects
                    select new Result {Doc = obj};
            }
        }
    
        [Fact]
        public async Task TestCase()
        {
            using var store = GetDocumentStore();
            await store.ExecuteIndexAsync(new TestIndex());
            var re = store.GetRequestExecutor();
            var c = re.HttpClient;
        
            var response = await re.HttpClient.GetAsync(new Uri($"{store.Urls.First()}/databases/{store.Database}/indexes/suggest-index-merge"));
            Assert.True(response.IsSuccessStatusCode);
        }

        private record AutoIndexMockup(string Name, int Count);

        private IndexMergeResults GetMergeReportOfTwoIndexes(IndexDefinition index1, IndexDefinition index2)
        {
            using var store = GetDocumentStore();
            store.Maintenance.Send(new PutIndexesOperation(index1, index2));
            var dictionary = new Dictionary<string, IndexDefinition>
            {
                {index1.Name, index1},
                {index2.Name, index2}
            };

            var merger = new IndexMerger(dictionary);
            return merger.ProposeIndexMergeSuggestions();
        }

        private void AssertIndexIsNotCorruptingIndexMerger<TIndex>() where TIndex : AbstractIndexCreationTask, new()
        {
            using var store = GetDocumentStore();
            var index = new TIndex();
            index.Execute(store);
            var whereIndex = store
                .Maintenance
                .Send(new GetIndexOperation(index.IndexName));

            var dictionary = new Dictionary<string, IndexDefinition>
            {
                {index.IndexName, whereIndex},
            };
            var merger = new IndexMerger(dictionary);
            Assert.Equal(0, merger.ProposeIndexMergeSuggestions().Suggestions.Count);
            Assert.Equal(1, merger.ProposeIndexMergeSuggestions().Unmergables.Count);
        }

        [Fact]
        public void IndexDefinitionCanContainWhereInInvocationExpressionSyntax() => AssertIndexIsNotCorruptingIndexMerger<IndexWithWhere>();

        [Fact]
        public void IndexDefinitionCanContainLetInInvocationExpressionSyntax() => AssertIndexIsNotCorruptingIndexMerger<IndexWithSyntaxQueryLet>();

        [Fact]
        public void CanMergeWithConditionalStatementsAndParenthesis()
        {
            using var store = GetDocumentStore();
            var index1 = new IndexWithParenthesisAndConditionalStatementNested();
            var index2 = new IndexWithLiteral();
            index1.Execute(store);
            index2.Execute(store);
            var indexDefinition1 = store.Maintenance.Send(new GetIndexOperation(index1.IndexName));
            var indexDefinition2 = store.Maintenance.Send(new GetIndexOperation(index2.IndexName));
            var merger = new IndexMerger(new Dictionary<string, IndexDefinition>()
            {
                {index1.IndexName, indexDefinition1}, {index2.IndexName, indexDefinition2}
            });
           
            Assert.Equal(1, merger.ProposeIndexMergeSuggestions().Suggestions.Count);
            Assert.Equal(0, merger.ProposeIndexMergeSuggestions().Unmergables.Count);
            Assert.Equal("docs.AutoIndexMockups.Select(doc => new { Age = doc.Count > 0 ? -1 : doc.Count * 25, SecretField = doc.Name == null ? \"Test\" : doc.Name + \"Maciej\" })", merger.ProposeIndexMergeSuggestions().Suggestions[0].MergedIndex.Maps.First());
        }

        private class IndexWithParenthesisAndConditionalStatementNested : AbstractIndexCreationTask<AutoIndexMockup>
        {
            public IndexWithParenthesisAndConditionalStatementNested()
            {
                Map = mockups => mockups.Select(i => new
                {
                    SecretField = (i.Name == null ? "Test" : (i.Name + "Maciej" )),
                });
            }
        }
        
        private class IndexWithLiteral : AbstractIndexCreationTask<AutoIndexMockup>
        {
            public IndexWithLiteral()
            {
                Map = mockups => mockups.Select(i => new
                {
                    Age = i.Count > 0 ? -1 : i.Count * 25
                });
            }
        }
        
        private class IndexWithWhere : AbstractIndexCreationTask<AutoIndexMockup>
        {
            public IndexWithWhere()
            {
                Map = mockups => mockups.Where(i => i.Name == "Matt").Select(i => new { Name = i.Name });
            }
        }

        private class IndexWithSyntaxQueryLet : AbstractIndexCreationTask<AutoIndexMockup>
        {
            public IndexWithSyntaxQueryLet()
            {
                Map = mockups => from moc in mockups
                                 let x = "SuperField"
                                 select new { Name = moc.Name, X = x };
            }
        }
    }
}
