// -----------------------------------------------------------------------
//  <copyright file="DocumentsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace Raven.Tests.Core.Streaming
{
	public class DocumentStreaming : RavenCoreTestBase
	{
		[Fact]
		public void CanStreamDocumentsStartingWith()
		{
			using (var store = GetDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 200; i++)
					{
						session.Store(new User());
					}
					session.SaveChanges();
				}

				int count = 0;
				using (var session = store.OpenSession())
				{
					using (var reader = session.Advanced.Stream<User>(startsWith: "users/"))
					{
						while (reader.MoveNext())
						{
							count++;
							Assert.IsType<User>(reader.Current.Document);
						}
					}
				}
				Assert.Equal(200, count);
			}
		}

		[Fact]
		public void CanStreamDocumentsFromSpecifiedEtag()
		{
			using (var store = GetDocumentStore())
			{
				Etag fromEtag;

				using (var session = store.OpenSession())
				{
					User hundredthUser = null;

					for (int i = 0; i < 200; i++)
					{
						var user = new User();
						session.Store(user);

						if (i == 99)
						{
							hundredthUser = user;
						}
					}
					session.SaveChanges();

					fromEtag = session.Advanced.GetEtagFor(hundredthUser);
				}

				int count = 0;
				using (var session = store.OpenSession())
				{
					using (var reader = session.Advanced.Stream<User>(fromEtag: fromEtag))
					{
						while (reader.MoveNext())
						{
							count++;
							Assert.IsType<User>(reader.Current.Document);
						}
					}
				}
				Assert.Equal(100, count);
			}
		}
	}
}