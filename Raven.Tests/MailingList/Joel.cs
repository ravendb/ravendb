// -----------------------------------------------------------------------
//  <copyright file="Joel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Joel : RavenTest
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
				public object Query { get; set; }
			}

			public Index()
			{
				Map = items =>
					  from item in items
					  select new Result
					  {
						  Query = new object[] { item.Age, item.Name }
					  };
			}
		}

		[Fact]
		public void CanCreateIndexWithExplicitType()
		{
			using (var s = NewDocumentStore())
			{
				new Index().Execute(s);
				var indexDefinition = s.DocumentDatabase.IndexDefinitionStorage.GetIndexDefinition("Index");
				Assert.Contains("new()", indexDefinition.Map);
			}
		}
	}
}