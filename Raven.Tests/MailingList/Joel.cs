// -----------------------------------------------------------------------
//  <copyright file="Joel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Lucene.Net.Search;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Joel : RavenTest
	{
		private class Item
		{
			public string Name { get; set; }
			public int Age { get; set; }
		}

		private class Index : AbstractIndexCreationTask<Item, Index.Result>
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
					      Query = new object[] {item.Age, item.Name}
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
				Assert.Equal(@"docs.Items.Select(item => new {
    Query = new object[] {
        ((object) item.Age),
        item.Name
    }
})", indexDefinition.Map);
			}
		}
	}
}