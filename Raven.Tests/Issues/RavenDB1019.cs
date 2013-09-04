// -----------------------------------------------------------------------
//  <copyright file="RavenDB1019.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Bugs;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB1019 : RavenTest
	{
		[Fact]
		public void StreamDocsShouldWork()
		{
			using (var store = NewRemoteDocumentStore(runInMemory: false))
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User() { Name = "Test" });
					session.SaveChanges();
				}

				var enumerator = store.DatabaseCommands.StreamDocs();

				var count = 0;
				while (enumerator.MoveNext())
				{
					count++;
				}

				Assert.Equal(2, count);
			}
		}

		[Fact]
		public void CanDisposeEarly()
		{
			using (var store = NewDocumentStore(runInMemory: false))
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 1000; i++)
					{
						session.Store(new User() { Name = "Test" });
					}
					session.SaveChanges();
				}

				var enumerator = store.DatabaseCommands.StreamDocs();

				while (enumerator.MoveNext())
				{
					enumerator.Dispose();
					break;
				}
			}
		}
	}
}