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
                Assert.Equal(@"docs.Users.Select(doc => new
{
Age = doc.Age, Email = doc.Email, Name = doc.Name
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
                Assert.Equal(@"from doc in docs.Users
select new
{
Age = doc.Age, Email = doc.Email, Name = doc.Name
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
            Assert.Equal("Cannot merge indexes that have a where clause", results.Unmergables[index1.Name]);
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
            Assert.Equal(@"from doc in docs.Products
select new
{
Category = doc.Category, Name = doc.Name, PricePerUnit = doc.PricePerUnit, Supplier = doc.Supplier, UnitOnStock = LoadCompareExchangeValue(Id(doc))}"
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
            Assert.Equal(@"from doc in docs.Orders
select new
{
Company = doc.Company, Employee = doc.Employee, ShipmentLocation = CreateSpatialField(doc.ShipTo.Latitude, doc.ShipTo.Longitude), Total = doc.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))}", results.Suggestions.First().MergedIndex.Maps.First());
        }

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
    }
}
