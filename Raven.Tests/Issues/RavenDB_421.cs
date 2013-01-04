// -----------------------------------------------------------------------
//  <copyright file="RavenDB_421.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_421 : RavenTest
	{
		public class Person
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string[] Parents { get; set; }
		}

		public class Family : AbstractMultiMapIndexCreationTask<Family.Result>
		{
			public class Result
			{
				public string PersonId { get; set; }
				public string Name { get; set; }
				public Child[] Children { get; set; }
			}

			public class Child
			{
				public string Id { get; set; }
				public string Name { get; set; }

			}

			public Family()
			{
				AddMap<Person>(people =>
							   from person in people
							   select new
							   {
								   PersonId = person.Id,
								   person.Name,
								   Children = new object[0]
							   });
				AddMap<Person>(people =>
							   from person in people
							   from parent in person.Parents
							   select new
							   {
								   PersonId = parent,
								   Name = (string)null,
								   Children = new[] { new { person.Name, person.Id } }
							   });

				Reduce = results =>
						 from result in results
						 group result by result.PersonId
							 into g
							 select new
							 {
								 PersonId = g.Key,
								 g.FirstOrDefault(x => x.Name != null).Name,
								 Children = g.SelectMany(x => x.Children)
							 };
			}

		}

		[Fact]
		public void CanExecuteIndexWithoutNRE()
		{
			using (var store = NewDocumentStore())
			{
				new Family().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Person
					{
						Name = "Parent",
						Parents = new string[0]
					});
					session.Store(new Person
					{
						Name = "Child",
						Parents = new[] { "people/1", "people/123" }
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var results = session.Query<Family.Result, Family>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.PersonId == "people/1")
						.ToList();

					WaitForUserToContinueTheTest(store);
					Assert.Empty(store.DocumentDatabase.Statistics.Errors);
					Assert.Equal(1, results[0].Children.Length);

				}
			}
		}

	}
}