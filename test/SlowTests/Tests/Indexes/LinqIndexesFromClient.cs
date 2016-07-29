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
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class LinqIndexesFromClient : RavenTestBase
    {
        [Fact]
        public void Convert_select_many_will_keep_doc_id()
        {
            IndexDefinition indexDefinition = new IndexDefinitionBuilder<Order>
            {
                Map = orders => from order in orders
                                from line in order.OrderLines
                                select new { line.ProductId }
            }.ToIndexDefinition(new DocumentConvention { PrettifyGeneratedLinqExpressions = false });

            indexDefinition.Name = "Index1";
            var index = IndexAndTransformerCompiler.Compile(indexDefinition);
            var map = index.Maps.Values.First();

            using (var context = new JsonOperationContext(new UnmanagedBuffersPool(string.Empty)))
            {
                var results = map(new[]
                {
                GetDocumentFromString(
                @"
                {
                    '@metadata': {'Raven-Entity-Name': 'Orders', '@id': 1},
                    'OrderLines': [{'ProductId': 2}, {'ProductId': 3}]
                }", context),
                  GetDocumentFromString(
                @"
                {
                    '@metadata': {'Raven-Entity-Name': 'Orders', '@id': 2},
                    'OrderLines': [{'ProductId': 5}, {'ProductId': 4}]
                }", context)
            }).Cast<object>().ToArray();

                var fields = index.OutputFields
                    .Select(x => IndexField.Create(x, new IndexFieldOptions(), null))
                    .ToList();

                var converter = new AnonymousLuceneDocumentConverter(fields);
                foreach (var result in results)
                {
                    var doc = converter.ConvertToCachedDocument(context.GetLazyString("docs/1"), result);
                    Assert.Equal("docs/1", doc.Get("__document_id"));
                }
            }
        }

        public static dynamic GetDocumentFromString(string json, JsonOperationContext context)
        {
            var ms = new MemoryStream(Encoding.UTF8.GetBytes(json));
            var reader = context.ReadForMemory(ms, "doc");

            return new DynamicBlittableJson(new Document
            {
                Data = reader
            });
        }

        [Fact(Skip = "https://github.com/dotnet/roslyn/issues/12045")]
        public void CanCompileComplexQuery()
        {
            var indexDefinition = new IndexDefinitionBuilder<Person>()
            {
                Map = people => from person in people
                                from role in person.Roles
                                where role == "Student"
                                select new { role }
            }.ToIndexDefinition(new DocumentConvention { PrettifyGeneratedLinqExpressions = false });

            indexDefinition.Name = "Index1";
            IndexAndTransformerCompiler.Compile(indexDefinition);
        }

        public class Person
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
            }.ToIndexDefinition(new DocumentConvention { PrettifyGeneratedLinqExpressions = false });

            var original = new IndexDefinition
            {
                Fields =
                {
                    { "Name", new IndexFieldOptions {Storage = FieldStorage.Yes} }
                },
                Maps = { @"docs.Users.Where(user => user.Location == ""Tel Aviv"").Select(user => new {
    Name = user.Name
})" }
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
            }.ToIndexDefinition(new DocumentConvention { PrettifyGeneratedLinqExpressions = false });
            var original = new IndexDefinition
            {
                Fields =
                {
                    { "Name", new IndexFieldOptions {Storage = FieldStorage.Yes} }
                },
                Maps = { @"docs.Users.Where(user => user.Location == ""Tel Aviv"").Select(user => new {
    Age = user.Age - (20 - user.Age)
})" }
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
            }.ToIndexDefinition(new DocumentConvention { PrettifyGeneratedLinqExpressions = false });
            var original = new IndexDefinition
            {
                Fields =
                {
                    {"Name", new IndexFieldOptions {Storage = FieldStorage.Yes}}
                },
                Maps = { @"docs.Users.Where(user => user.Location == ""Tel Aviv"").Select(user => new {
    Name = user.Name,
    Id = user.__document_id
})" }
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
            }.ToIndexDefinition(new DocumentConvention { PrettifyGeneratedLinqExpressions = false });
            var original = new IndexDefinition
            {
                Fields =
                {
                    {"Name", new IndexFieldOptions {Storage = FieldStorage.Yes}}
                },
                Maps = { @"docs.Users.Where(user => !(user.Location == ""Te(l) (A)viv"")).Select(user => new {
    Name = user.Name
})" }
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
            }.ToIndexDefinition(new DocumentConvention { PrettifyGeneratedLinqExpressions = false });
            var original = new IndexDefinition
            {
                Maps = { "docs.Users.Where(user => Enumerable.Contains(user.Name, 'C'))" }
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
            }.ToIndexDefinition(new DocumentConvention { PrettifyGeneratedLinqExpressions = false });
            var original = new IndexDefinition
            {
                Maps = { @"docs.Users.Select(user => new {
    Location = user.Location,
    Count = 1
})" },
                Reduce = @"results.GroupBy(agg => agg.Location).Select(g => new {
    Location = g.Key,
    Count = Enumerable.Sum(g, x => ((int) x.Count))
})"
            };

            Assert.True(original.Maps.SetEquals(generated.Maps));
            Assert.Equal(original.Reduce, generated.Reduce);
        }

        public void Convert_map_reduce_query_with_map_(Expression<Func<IEnumerable<User>, IEnumerable>> mapExpression, string expectedIndexString)
        {
            IndexDefinition generated = new IndexDefinitionBuilder<User, LocationCount>
            {
                Map = mapExpression,
                Reduce = counts => from agg in counts
                                   group agg by agg.Location
                                       into g
                                   select new { Location = g.Key, Count = g.Sum(x => x.Count) },
            }.ToIndexDefinition(new DocumentConvention { PrettifyGeneratedLinqExpressions = false });
            var original = new IndexDefinition
            {
                Maps = { expectedIndexString },
                Reduce = @"results.GroupBy(agg => agg.Location).Select(g => new {
    Location = g.Key,
    Count = Enumerable.Sum(g, x => ((int) x.Count))
})"
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
})");
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
})");
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
})");
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
})");
        }


        public enum Gender
        {
            Male,
            Female
        }

        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Location { get; set; }

            public int Age { get; set; }
            public Gender Gender { get; set; }
        }

        public class Named
        {
            public string Name { get; set; }
        }
        public class LocationCount
        {
            public string Location { get; set; }
            public int Count { get; set; }
        }


        public class LocationAge
        {
            public string Location { get; set; }
            public decimal AverageAge { get; set; }
            public int Count { get; set; }
            public decimal AgeSum { get; set; }
        }

        public class Order
        {
            public string Id { get; set; }
            public string Customer { get; set; }
            public IList<OrderLine> OrderLines { get; set; }

            public Order()
            {
                OrderLines = new List<OrderLine>();
            }
        }

        public class OrderLine
        {
            public string ProductId { get; set; }
            public int Quantity { get; set; }
        }
    }
}
