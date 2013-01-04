// -----------------------------------------------------------------------
//  <copyright file="RavenDB_367.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_367 : RavenTest
	{
		[Fact]
		public void CanFetchAllStoredFields()
		{
			using(var store = NewDocumentStore())
			{
				new Index().Execute(store);
				using(var session = store.OpenSession())
				{
					session.Store(new Item{Name = "Oren Eini"});
					session.SaveChanges();
				}

				using(var session= store.OpenSession())
				{
					var objects = session.Advanced.LuceneQuery<dynamic>("Index")
						.WaitForNonStaleResults()
						.SelectFields<dynamic>(Constants.AllFields)
						.ToList();

					Assert.Equal("Oren", objects[0].First);
					Assert.Equal("Eini", objects[0].Last);
				}
			}
		}

		public class Item
		{
			public string Name { get; set; }
		}

		public class Index : AbstractIndexCreationTask<Item>
		{
			public Index()
			{
				Map = items =>
				      from item in items
				      let names = item.Name.Split()
				      select new {First = names[0], Last = names[1]};

				StoreAllFields(FieldStorage.Yes);
			}
		}
	}
}