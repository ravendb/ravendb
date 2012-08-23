//-----------------------------------------------------------------------
// <copyright file="QueryingByNull.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryingByNull : RavenTest
	{
		[Fact]
		public void CanQueryByNullUsingLinq()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Person());
					session.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("People/ByName",
				                                new IndexDefinition
				                                {
				                                	Map = "from doc in docs.People select new { doc.Name}"
				                                });

				using(var session = store.OpenSession())
				{
					var q = from person in session.Query<Person>("People/ByName")
								.Customize(x=>x.WaitForNonStaleResults())
							where person.Name == null
					        select person;
					Assert.Equal(1, q.Count());
				}
			}
		}

		public class Person
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
	}
}
