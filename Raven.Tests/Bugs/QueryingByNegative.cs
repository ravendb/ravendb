//-----------------------------------------------------------------------
// <copyright file="QueryingByNegative.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryingByNegative : RavenTest
	{
		[Fact]
		public void CanQueryByNullUsingLinq()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Person
					{
						Age = -5
					});
					session.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("People/ByAge",
												new IndexDefinition
												{
													Map = "from doc in docs.People select new { doc.Age}",
													Indexes= {{"Age", FieldIndexing.NotAnalyzed}}
												});

				using (var session = store.OpenSession())
				{
					var q = from person in session.Query<Person>("People/ByAge")
						.Customize(x => x.WaitForNonStaleResults())
							where person.Age == -5
							select person;
					Assert.Equal(1, q.Count());
				}
			}
		}

		public class Person
		{
			public string Id { get; set; }
			public int Age { get; set; }
		}
	}
}
