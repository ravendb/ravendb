// -----------------------------------------------------------------------
//  <copyright file="RavenDB921.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Issues
{
	public class RavenDB937 : RavenTest
	{
		public class User
		{
			public bool Active { get; set; }
		}

		[Fact]
		public void LowLevelRemoteStreamAsync()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 1500; i++)
					{
						session.Store(new User());
					}
					session.SaveChanges();
				}

				WaitForIndexing(store);
				
				var queryHeaderInfo = new Reference<QueryHeaderInformation>();
				var enumerator =
					store.AsyncDatabaseCommands.StreamQueryAsync(new RavenDocumentsByEntityName().IndexName, new IndexQuery
					{
						Query = "",
						SortedFields = new[] {new SortedField(Constants.DocumentIdFieldName),}
					}, queryHeaderInfo).Result;

				Assert.Equal(1500, queryHeaderInfo.Value.TotalResults);

				int count = 0;
				while (enumerator.MoveNextAsync().Result)
				{
					count++;
				}

				Assert.Equal(1500, count);
			}
		}

		[Fact]
		public void HighLevelRemoteStreamAsync()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 1500; i++)
					{
						session.Store(new User());
					}
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenAsyncSession())
				{
					var enumerator = session.Advanced.StreamAsync(session.Query<User>(new RavenDocumentsByEntityName().IndexName)).Result;
					int count = 0;
					while (enumerator.MoveNextAsync().Result)
					{
						Assert.IsType<User>(enumerator.Current.Document);
						count++;
					}

					Assert.Equal(1500, count);
				}
			}
		}

		[Fact]
		public void HighLevelLocalStreamWithFilterAsync()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("Users/ByActive",
				                                new IndexDefinition
				                                {
					                                Map = "from u in docs.Users select new { u.Active}"
				                                });

				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 3000; i++)
					{
						session.Store(new User
						{
							Active = i % 2 == 0
						});
					}
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenAsyncSession())
				{
					var query = session.Query<User>("Users/ByActive")
					                   .Where(x => x.Active);
					var enumerator = session.Advanced.StreamAsync(query).Result;
					int count = 0;
					while (enumerator.MoveNextAsync().Result)
					{
						Assert.IsType<User>(enumerator.Current.Document);
						count++;
					}

					Assert.Equal(1500, count);
				}
			}
		}

		[Fact]
		public void LowLevelEmbeddedStreamAsync()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 1500; i++)
					{
						session.Store(new User());
					}
					session.SaveChanges();
				}

				WaitForIndexing(store);

				var queryHeaderInfo = new Reference<QueryHeaderInformation>();
				var enumerator = store.AsyncDatabaseCommands.StreamQueryAsync(new RavenDocumentsByEntityName().IndexName, new IndexQuery
				{
					Query = "",
					SortedFields = new[] { new SortedField(Constants.DocumentIdFieldName), }
				}, queryHeaderInfo).Result;

				Assert.Equal(1500, queryHeaderInfo.Value.TotalResults);

				int count = 0;
				while (enumerator.MoveNextAsync().Result)
				{
					count++;
				}

				Assert.Equal(1500, count);
			}
		}
	}
}