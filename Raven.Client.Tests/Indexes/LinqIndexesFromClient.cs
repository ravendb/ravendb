using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Tests.Document;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Linq;
using Xunit;

namespace Raven.Client.Tests.Indexes
{
	public class LinqIndexesFromClient
	{
        [Fact]
        public void Convert_select_many_will_keep_doc_id()
        {
            IndexDefinition indexDefinition = new IndexDefinition<Order>
            {
                Map = orders => from order in orders
                                from line in order.OrderLines
                                select new { line.ProductId }
            }.ToIndexDefinition(new DocumentConvention());
            var generator = new DynamicViewCompiler("test", indexDefinition)
                .GenerateInstance();


            var results = generator.MapDefinition(new[]
			{
				GetDocumentFromString(
				@"
                {
                    '@metadata': {'X-Raven-Entity-Name': 'Orders', '@id': 1},
                    'OrderLines': [{'ProductId': 2}, {'ProductId': 3}]
                }"),
				  GetDocumentFromString(
				@"
                {
                    '@metadata': {'X-Raven-Entity-Name': 'Orders', '@id': 2},
                    'OrderLines': [{'ProductId': 5}, {'ProductId': 4}]
                }")
			}).Cast<object>().ToArray();

            foreach (var result in results)
            {
                Assert.NotNull(TypeDescriptor.GetProperties(result).Find("__document_id", true));
            }
        }

        public static dynamic GetDocumentFromString(string json)
        {
            return JsonToExpando.Convert(JObject.Parse(json));
        }

	    [Fact]
		public void Convert_simple_query()
		{
			IndexDefinition generated = new IndexDefinition<User, Named>
			{
				Map = users => from user in users
							   where user.Location == "Tel Aviv"
							   select new { user.Name },
				Stores = { { user => user.Name, FieldStorage.Yes } }
			}.ToIndexDefinition(new DocumentConvention());
			var original = new IndexDefinition
			{
				Stores = { { "Name", FieldStorage.Yes } },
				Map = @"docs.Users
	.Where(user => (user.Location == ""Tel Aviv""))
	.Select(user => new {Name = user.Name})"
			};

			Assert.Equal(original, generated);
		}

		[Fact]
		public void Convert_map_reduce_query()
		{
			IndexDefinition generated = new IndexDefinition<User, LocationCount>
			{
				Map = users => from user in users
							   select new { user.Location, Count = 1 },
				Reduce = counts => from agg in counts
								   group agg by agg.Location
									   into g
									   select new { Location = g.Key, Count = g.Sum(x => x.Count) },
			}.ToIndexDefinition(new DocumentConvention());
			var original = new IndexDefinition
			{
				Map = @"docs.Users
	.Select(user => new {Location = user.Location, Count = 1})",
				Reduce = @"results
	.GroupBy(agg => agg.Location)
	.Select(g => new {Location = g.Key, Count = g.Sum(x => x.Count)})"
			};

			Assert.Equal(original, generated);
		}


		public class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Location { get; set; }

			public int Age { get; set; }
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
			public decimal Age { get; set; }
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