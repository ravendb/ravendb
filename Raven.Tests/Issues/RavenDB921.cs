// -----------------------------------------------------------------------
//  <copyright file="RavenDB921.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB921 : RavenTest
	{
		public class User
		{
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
				var enumerator = store.DatabaseCommands.Query(new RavenDocumentsByEntityName().IndexName, new IndexQuery
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
				var enumerator = store.DatabaseCommands.Query(new RavenDocumentsByEntityName().IndexName, new IndexQuery
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