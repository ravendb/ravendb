// -----------------------------------------------------------------------
//  <copyright file="Lars.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Lars : RavenTest
	{
		public class Item
		{
			public string Name { get; set; }
			public int Age { get; set; }
		}

		public class Index : AbstractIndexCreationTask<Item, Index.Result>
		{
			public class Result
			{
				public int Age { get; set; }
			}

			public Index()
			{
				Map = items =>
					  from item in items
					  select new
					  {
						  item.Age
					  };

				Reduce = results =>
						 from r in results
						 group r by 1
							 into g
							 let items = g.ToArray()
							 select new { Age = items.Sum(x => x.Age) };
			}
		}

		[Fact]
		public void EnumerableMethodsShouldBeExtenalStaticCalls()
		{
			using (var s = NewDocumentStore())
			{
				new Index().Execute(s);
				var indexDefinition = s.DocumentDatabase.IndexDefinitionStorage.GetIndexDefinition("Index");
				Assert.Equal("results\r\n\t.GroupBy(r => 1)\r\n\t.Select(g => new {g = g, items = Enumerable.ToArray(g)})\r\n\t.Select(__h__TransparentIdentifier0 => new {Age = Enumerable.Sum(__h__TransparentIdentifier0.items, x => ((System.Int32)(x.Age)))})", indexDefinition.Reduce);
			}
		}
	}
}