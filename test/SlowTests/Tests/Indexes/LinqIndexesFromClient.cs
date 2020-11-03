//-----------------------------------------------------------------------
// <copyright file="LinqIndexesFromClient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;
using static FastTests.Server.Documents.Indexing.Lucene.LuceneDocumentConverterTests;

namespace SlowTests.Tests.Indexes
{
    public class LinqIndexesFromClient : RavenTestBase
    {
        public LinqIndexesFromClient(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Convert_select_many_will_keep_doc_id()
        {
            var indexDefinition = new IndexDefinitionBuilder<Order>
            {
                Map = orders => from order in orders
                                from line in order.OrderLines
                                select new { line.ProductId }
            }.ToIndexDefinition(DocumentConventions.Default);

            indexDefinition.Name = "Index1";
            var index = (StaticIndexBase)IndexCompiler.Compile(indexDefinition);

            var map = index.Maps.Values.First().First().Value.First();

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var results = map(new[]
                {
                GetDocumentFromString(
                @"
                {
                    '@metadata': {'@collection': 'Orders', '@id': 1},
                    'OrderLines': [{'ProductId': 2}, {'ProductId': 3}]
                }".Replace("\r\n", Environment.NewLine), context),
                  GetDocumentFromString(
                @"
                {
                    '@metadata': {'@collection': 'Orders', '@id': 2},
                    'OrderLines': [{'ProductId': 5}, {'ProductId': 4}]
                }".Replace("\r\n", Environment.NewLine), context)
            }).Cast<object>().ToArray();

                var fields = index.OutputFields
                    .Select(x => IndexField.Create(x, new IndexFieldOptions(), null))
                    .ToList();

#pragma warning disable CS0618 // Type or member is obsolete
                var converter = new AnonymousLuceneDocumentConverter(new FakeIndex(), fields, false);
#pragma warning restore CS0618 // Type or member is obsolete
                foreach (var result in results)
                {
                    using (var lazyStringValue = context.GetLazyString("docs/1"))
                    {
                        bool shouldSkip;
                        converter.SetDocument(lazyStringValue, null, result, context, FastTests.Server.Documents.Indexing.Lucene.LuceneDocumentConverterTests.FakeWriteOperationBuffer.Create(), out shouldSkip);
                        Assert.Equal("docs/1", converter.Document.Get(Constants.Documents.Indexing.Fields.DocumentIdFieldName, null));
                    }
                }
            }
        }

        private static dynamic GetDocumentFromString(string json, JsonOperationContext context)
        {
            var ms = new MemoryStream(Encoding.UTF8.GetBytes(json));
            var reader = context.Sync.ReadForMemory(ms, "doc");

            return new DynamicBlittableJson(new Document
            {
                Data = reader
            });
        }

        [Fact]
        public void CanCompileComplexQuery()
        {
            var indexDefinition = new IndexDefinitionBuilder<Person>()
            {
                Map = people => from person in people
                                from role in person.Roles
                                where role == "Student"
                                select new { role }
            }.ToIndexDefinition(DocumentConventions.Default);

            indexDefinition.Name = "Index1";
            IndexCompiler.Compile(indexDefinition);
        }

        private class Person
        {
            public string[] Roles { get; set; }
        }

        [Fact]
        public void Convert_simple_query()
        {
            IndexDefinition generated = new IndexDefinitionBuilder<User, Named>
            {
                Map = users => from user in users
                               where user.Location == "Tel Aviv"
                               select new { user.Name },
                Stores = { { user => user.Name, FieldStorage.Yes } }
            }.ToIndexDefinition(DocumentConventions.Default);

            var original = new IndexDefinition
            {
                Fields =
                {
                    { "Name", new IndexFieldOptions {Storage = FieldStorage.Yes} }
                },
                Maps = { @"docs.Users.Where(user => user.Location == ""Tel Aviv"").Select(user => new {
    Name = user.Name
})".Replace("\r\n", Environment.NewLine) }
            };

            Assert.True(original.Maps.SetEquals(generated.Maps));
        }

        [Fact]
        public void With_parantesis()
        {
            IndexDefinition generated = new IndexDefinitionBuilder<User, Named>
            {
                Map = users => from user in users
                               where user.Location == "Tel Aviv"
                               select new { Age = user.Age - (20 - user.Age) },
                Stores = { { user => user.Name, FieldStorage.Yes } }
            }.ToIndexDefinition(DocumentConventions.Default);

            var original = new IndexDefinition
            {
                Fields =
                {
                    { "Name", new IndexFieldOptions {Storage = FieldStorage.Yes} }
                },
                Maps = { @"docs.Users.Where(user => user.Location == ""Tel Aviv"").Select(user => new {
    Age = user.Age - (20 - user.Age)
})".Replace("\r\n", Environment.NewLine) }
            };

            Assert.True(original.Maps.SetEquals(generated.Maps));
        }

        [Fact]
        public void Convert_using_id()
        {
            IndexDefinition generated = new IndexDefinitionBuilder<User, Named>
            {
                Map = users => from user in users
                               where user.Location == "Tel Aviv"
                               select new { user.Name, user.Id },
                Stores = { { user => user.Name, FieldStorage.Yes } }
            }.ToIndexDefinition(DocumentConventions.Default);

            var original = new IndexDefinition
            {
                Fields =
                {
                    {"Name", new IndexFieldOptions {Storage = FieldStorage.Yes}}
                },
                Maps = { @"docs.Users.Where(user => user.Location == ""Tel Aviv"").Select(user => new {
    Name = user.Name,
    Id = Id(user)
})".Replace("\r\n", Environment.NewLine) }
            };

            Assert.True(original.Maps.SetEquals(generated.Maps));
            Assert.Equal(original, generated);
        }

        [Fact]
        public void Convert_simple_query_with_not_operator_and_nested_braces()
        {
            IndexDefinition generated = new IndexDefinitionBuilder<User, Named>
            {
                Map = users => from user in users
                               where !(user.Location == "Te(l) (A)viv")
                               select new { user.Name },
                Stores = { { user => user.Name, FieldStorage.Yes } }
            }.ToIndexDefinition(DocumentConventions.Default);

            var original = new IndexDefinition
            {
                Fields =
                {
                    {"Name", new IndexFieldOptions {Storage = FieldStorage.Yes}}
                },
                Maps = { @"docs.Users.Where(user => !(user.Location == ""Te(l) (A)viv"")).Select(user => new {
    Name = user.Name
})".Replace("\r\n", Environment.NewLine) }
            };

            Assert.True(original.Maps.SetEquals(generated.Maps));
            Assert.Equal(original, generated);
        }

        [Fact]
        public void Convert_simple_query_with_char_literal()
        {
            IndexDefinition generated = new IndexDefinitionBuilder<User>
            {
                Map = users => from user in users
                               where user.Name.Contains('C')
                               select user
            }.ToIndexDefinition(DocumentConventions.Default);

            var original = new IndexDefinition
            {
                Maps = { "docs.Users.Where(user => user.Name.Contains('C'))" }
            };
            Assert.True(original.Maps.SetEquals(generated.Maps));
        }

        [Fact]
        public void Convert_map_reduce_query()
        {
            IndexDefinition generated = new IndexDefinitionBuilder<User, LocationCount>
            {
                Map = users => from user in users
                               select new { user.Location, Count = 1 },
                Reduce = counts => from agg in counts
                                   group agg by agg.Location
                                       into g
                                   select new { Location = g.Key, Count = g.Sum(x => x.Count) },
            }.ToIndexDefinition(DocumentConventions.Default);

            var original = new IndexDefinition
            {
                Maps = { @"docs.Users.Select(user => new {
    Location = user.Location,
    Count = 1
})".Replace("\r\n", Environment.NewLine) },
                Reduce = @"results.GroupBy(agg => agg.Location).Select(g => new {
    Location = g.Key,
    Count = Enumerable.Sum(g, x => ((int) x.Count))
})".Replace("\r\n", Environment.NewLine)
            };

            Assert.True(original.Maps.SetEquals(generated.Maps));
            Assert.Equal(original.Reduce, generated.Reduce);
        }

        private void Convert_map_reduce_query_with_map_(Expression<Func<IEnumerable<User>, IEnumerable>> mapExpression, string expectedIndexString)
        {
            IndexDefinition generated = new IndexDefinitionBuilder<User, LocationCount>
            {
                Map = mapExpression,
                Reduce = counts => from agg in counts
                                   group agg by agg.Location
                                       into g
                                   select new { Location = g.Key, Count = g.Sum(x => x.Count) },
            }.ToIndexDefinition(DocumentConventions.Default);

            var original = new IndexDefinition
            {
                Maps = { expectedIndexString },
                Reduce = @"results.GroupBy(agg => agg.Location).Select(g => new {
    Location = g.Key,
    Count = Enumerable.Sum(g, x => ((int) x.Count))
})".Replace("\r\n", Environment.NewLine)
            };

            Assert.Equal(expectedIndexString, generated.Maps.First());
            Assert.Equal(original.Reduce, generated.Reduce);
        }

        [Fact]
        public void Convert_map_reduce_preserving_parenthesis()
        {
            Convert_map_reduce_query_with_map_(
users => from user in users
         select new { Location = user.Location, Count = (user.Age + 3) * (user.Age + 4) },
@"docs.Users.Select(user => new {
    Location = user.Location,
    Count = (user.Age + 3) * (user.Age + 4)
})".Replace("\r\n", Environment.NewLine));
        }

        [Fact]
        public void Convert_map_reduce_query_with_trinary_conditional()
        {
            Convert_map_reduce_query_with_map_(
users => from user in users
         select new { Location = user.Location, Count = user.Age >= 1 ? 1 : 0 },
@"docs.Users.Select(user => new {
    Location = user.Location,
    Count = user.Age >= 1 ? 1 : 0
})".Replace("\r\n", Environment.NewLine));
        }

        [Fact]
        public void Convert_map_reduce_query_with_enum_comparison()
        {
            Convert_map_reduce_query_with_map_(
users => from user in users
         select new { Location = user.Location, Count = user.Gender == Gender.Female ? 1 : 0 },
@"docs.Users.Select(user => new {
    Location = user.Location,
    Count = user.Gender == ""Female"" ? 1 : 0
})".Replace("\r\n", Environment.NewLine));
        }

        [Fact]
        public void Convert_map_reduce_query_with_type_check()
        {
            Convert_map_reduce_query_with_map_(
users => from user in users
         select new { Location = user.Location, Count = user.Location is String ? 1 : 0 },
@"docs.Users.Select(user => new {
    Location = user.Location,
    Count = user.Location is String ? 1 : 0
})".Replace("\r\n", Environment.NewLine));
        }

        private enum Gender
        {
            Male,
            Female
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Location { get; set; }

            public int Age { get; set; }
            public Gender Gender { get; set; }
        }

        private class Named
        {
            public string Name { get; set; }
        }

        private class LocationCount
        {
            public string Location { get; set; }
            public int Count { get; set; }
        }

        private class LocationAge
        {
            public string Location { get; set; }
            public decimal AverageAge { get; set; }
            public int Count { get; set; }
            public decimal AgeSum { get; set; }
        }

        private class Order
        {
            public string Id { get; set; }
            public string Customer { get; set; }
            public IList<OrderLine> OrderLines { get; set; }

            public Order()
            {
                OrderLines = new List<OrderLine>();
            }
        }

        private class OrderLine
        {
            public string ProductId { get; set; }
            public int Quantity { get; set; }
        }
    }
}
