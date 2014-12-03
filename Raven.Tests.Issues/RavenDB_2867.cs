// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2867.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2867 : RavenTest
	{
		private const string Prefix = "Raven/Indexes/Replace/";

		private class Person
		{
			public string Id { get; set; }

			public string FirstName { get; set; }

			public string LastName { get; set; }
		}

		private class OldIndex : AbstractIndexCreationTask<Person>
		{
			public OldIndex()
			{
				Map = persons => from person in persons select new { person.FirstName };
			}
		}

		private class NewIndex : AbstractIndexCreationTask<Person>
		{
			public NewIndex()
			{
				Map = persons => from person in persons select new { person.FirstName, person.LastName };
			}
		}

		[Fact]
		public void T1()
		{
			using (var store = NewDocumentStore(runInMemory: false))
			{
				new OldIndex().Execute(store);
				new NewIndex().Execute(store);

				store
					.DatabaseCommands
					.Put(Prefix + new NewIndex().IndexName, null, RavenJObject.FromObject(new IndexReplaceDocument { IndexToReplace = new OldIndex().IndexName, MinimumEtagBeforeReplace = null }), new RavenJObject());

				var e = Assert.Throws<InvalidOperationException>(() =>
				{
					using (var session = store.OpenSession())
					{
						var count = session.Query<Person, OldIndex>()
							.Count(x => x.LastName == "Doe");
					}
				});

				Assert.Contains("The field 'LastName' is not indexed, cannot query on fields that are not indexed", e.InnerException.Message);

				WaitForIndexing(store);

				store.SystemDatabase.IndexReplacer.ReplaceIndexes(store.SystemDatabase.IndexStorage.Indexes);

				using (var session = store.OpenSession())
				{
					var count = session.Query<Person, OldIndex>()
						.Count(x => x.LastName == "Doe");
				}
			}
		}
	}
}