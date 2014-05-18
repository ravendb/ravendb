// -----------------------------------------------------------------------
//  <copyright file="RavenDB921.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Issues
{
	public class RavenDB921 : RavenTest
	{
		public class User
		{
			public bool Active { get; set; }
		}

		[Fact]
		public void LowLevelRemoteStream()
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
				
				QueryHeaderInformation queryHeaders;
				var enumerator = store.DatabaseCommands.StreamQuery(new RavenDocumentsByEntityName().IndexName, new IndexQuery
				{
					Query = "",
					SortedFields = new[]{new SortedField(Constants.DocumentIdFieldName), }
				}, out queryHeaders);

				Assert.Equal(1500, queryHeaders.TotalResults);

				int count = 0;
				while (enumerator.MoveNext())
				{
					count++;
				}

				Assert.Equal(1500, count);
			}
		}

		[Fact]
		public void HighLevelRemoteStream()
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

				using (var session = store.OpenSession())
				{
					var enumerator = session.Advanced.Stream(session.Query<User>(new RavenDocumentsByEntityName().IndexName));
					int count = 0;
					while (enumerator.MoveNext())
					{
						Assert.IsType<User>(enumerator.Current.Document);
						count++;
					}

					Assert.Equal(1500, count);
				}
			}
		}

		[Fact]
		public void HighLevelLocalStreamWithFilter()
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

				using (var session = store.OpenSession())
				{
					var query = session.Query<User>("Users/ByActive")
					                   .Where(x => x.Active);
					var enumerator = session.Advanced.Stream(query);
					int count = 0;
					while (enumerator.MoveNext())
					{
						Assert.IsType<User>(enumerator.Current.Document);
						count++;
					}

					Assert.Equal(1500, count);
				}
			}
		}

		[Fact]
		public void LowLevelEmbeddedStream()
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

				QueryHeaderInformation queryHeaders;
				var enumerator = store.DatabaseCommands.StreamQuery(new RavenDocumentsByEntityName().IndexName, new IndexQuery
				{
					Query = "",
					SortedFields = new[] { new SortedField(Constants.DocumentIdFieldName), }
				}, out queryHeaders);

				Assert.Equal(1500, queryHeaders.TotalResults);

				int count = 0;
				while (enumerator.MoveNext())
				{
					count++;
				}

				Assert.Equal(1500, count);
			}
		}
	}
}