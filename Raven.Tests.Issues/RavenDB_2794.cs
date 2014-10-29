// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2794.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2794 : RavenTest
	{
		private class Person
		{
			public string FirstName { get; set; }

			public string LastName { get; set; }
		}

		private class People_FirstName : AbstractTransformerCreationTask<Person>
		{
			public class Result
			{
				public string FirstName { get; set; }
			}

			public People_FirstName()
			{
				TransformResults = people => from person in people
											 select new
													{
														FirstName = person.FirstName
													};
			}
		}

		[Fact]
		public void LazyLoadWithTransformerShouldReturnNullIfThereAreNoResults()
		{
			using (var store = NewRemoteDocumentStore())
			{
				new People_FirstName().Execute(store);

				using (var session = store.OpenSession())
				{
					var lazy1 = session
						.Advanced
						.Lazily
						.Load<People_FirstName, Person>("people/1");

					var value1 = lazy1.Value;

					Assert.Null(value1);

					var lazy2 = session
						.Advanced
						.Lazily
						.Load<People_FirstName, Person>(new[] { "people/1" });

					var value2 = lazy2.Value;

					Assert.NotNull(value2);
					Assert.Equal(1, value2.Length);
					Assert.Null(value2[0]);
				}
			}
		}
	}
}