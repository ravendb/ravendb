using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Raven.Client.Document;
using Raven.Client.Tests.Document;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using Xunit;

namespace Raven.Client.Tests.Indexes
{
	public class LinqIndexesFromClient
	{
        [Fact]
        public void Convert_select_many()
        {
            IndexDefinition indexDefinition = new IndexDefinition<Order>
            {
                Map = orders => from order in orders
                                from line in order.OrderLines
                                select new { line.ProductId }
            }.ToIndexDefinition(new DocumentConvention());
            new DynamicViewCompiler("test", indexDefinition)
                .GenerateInstance();
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
									   select new { Loction = g.Key, Count = g.Sum(x => x.Count) },
			}.ToIndexDefinition(new DocumentConvention());
			var original = new IndexDefinition
			{
				Map = @"docs.Users
	.Select(user => new {Location = user.Location, Count = 1})",
				Reduce = @"results
	.GroupBy(agg => agg.Location)
	.Select(g => new {Loction = g.Key, Count = g.Sum(x => x.Count}))"
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