using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class NullCoalasing : RavenTest
	{
		[Fact]
		public void ShouldWork()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("test/1", null,
				                           RavenJObject.Parse("{ FirstName: 'Bob',LastName: 'Smith', MiddleInitial: 'Q' }"),
				                           new RavenJObject());
				store.DatabaseCommands.Put("test/2", null,
										   RavenJObject.Parse("{ FirstName: 'Bob',LastName: 'Smith', MiddleInitial: null }"),
										   new RavenJObject());

				store.DatabaseCommands.Put("test/3", null,
										   RavenJObject.Parse("{ FirstName: null, LastName: 'Smith', MiddleInitial: 'Q' }"),
										   new RavenJObject());


				store.DatabaseCommands.PutIndex("testByLastName", new IndexDefinition
				{
					Map = @"from doc in docs
							select new { 
								FirstName = doc.FirstName ?? string.Empty, 
								LastName = doc.LastName, 
								MiddleInitial = doc.MiddleInitial == null ? string.Empty : doc.MiddleInitial
							};",
					Stores =
				                                                  	{
				                                                  		{"FirstName", FieldStorage.Yes},
																		{"MiddleInitial", FieldStorage.Yes},
																		{"LastName", FieldStorage.Yes}
				                                                  	}

				});

				QueryResult queryResult;
				do
				{
					queryResult = store.DatabaseCommands.Query("testByLastName", new IndexQuery
					{
						FieldsToFetch = new[] {"FirstName", "LastName", "MiddleInitial"},
						SortedFields = new[]{new SortedField("__document_id"), }
					}, new string[0]);
					if (queryResult.IsStale)
						Thread.Sleep(100);
				} while (queryResult.IsStale);

				Assert.Equal(3, queryResult.Results.Count);

				Assert.Equal("Q", queryResult.Results[0].Value<string>("MiddleInitial"));
				Assert.Equal("", queryResult.Results[1].Value<string>("MiddleInitial"));
				Assert.Equal("Q", queryResult.Results[2].Value<string>("MiddleInitial"));

				Assert.Equal("Bob", queryResult.Results[0].Value<string>("FirstName"));
				Assert.Equal("Bob", queryResult.Results[1].Value<string>("FirstName"));
				Assert.Equal("", queryResult.Results[2].Value<string>("FirstName"));

				Assert.Equal("Smith", queryResult.Results[0].Value<string>("LastName"));
				Assert.Equal("Smith", queryResult.Results[1].Value<string>("LastName"));
				Assert.Equal("Smith", queryResult.Results[2].Value<string>("LastName"));
			}
		}
	}
}
