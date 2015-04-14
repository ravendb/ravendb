// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3344.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3344 : RavenTest
	{
		private class Index1 : AbstractIndexCreationTask<Person>
		{
			public class Result
			{
				public string CurrentName { get; set; }

				public string PreviousName { get; set; }
			}

			public Index1()
			{
				Map = persons => from person in persons
								 let metadata = MetadataFor(person)
								 from name in metadata.Value<string>("Names").Split(',')
								 select new
										{
											CurrentName = person.Name,
											PreviousName = person.Name
										};

				StoreAllFields(FieldStorage.Yes);
			}
		}

		[Fact]
		public void ShouldWork()
		{
			using (var store = NewRemoteDocumentStore())
			{
				new Index1().Execute(store);

				using (var session = store.OpenSession())
				{
					var person = new Person { Name = "John" };
					session.Store(person);
					var metadata = session.Advanced.GetMetadataFor(person);
					metadata["Names"] = "James,Jonathan";

					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var results = session
						.Query<Person, Index1>()
						.ProjectFromIndexFieldsInto<Index1.Result>()
						.ToList();

					Assert.Equal(2, results.Count);
				}
			}
		}
	}
}