// -----------------------------------------------------------------------
//  <copyright file="RavenDB_726.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
	using System.Linq;

	using Raven.Client;
	using Raven.Client.Linq;

	using Xunit;

	public class RavenDB_726 : RavenTest
	{
		[Fact]
		public void ProjectionsWorkWithQueries()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Test User" });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var query =
						s.Query<User>()
						 .Customize(c => c.WaitForNonStaleResults())
						 .Select(x => new Projection { UserName = x.Name });

					Assert.Equal("Test User", query.First().UserName);
				}
			}
		}

		[Fact]
		public void ProjectionsWorkWithLazyQueries()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Test User" });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var query =
						s.Query<User>()
						 .Customize(c => c.WaitForNonStaleResults())
						 .Select(x => new Projection { UserName = x.Name })
						 .Lazily();

					s.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

					Assert.Equal("Test User", query.Value.First().UserName);
				}
			}
		}

		[Fact]
		public void ProjectionsWorkWithQueriesRemote()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Test User" });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var query =
						s.Query<User>()
						 .Customize(c => c.WaitForNonStaleResults())
						 .Select(x => new Projection { UserName = x.Name });

					Assert.Equal("Test User", query.First().UserName);
				}
			}
		}

		[Fact]
		public void ProjectionsWorkWithLazyQueriesRemote()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Test User" });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var query =
						s.Query<User>()
						 .Customize(c => c.WaitForNonStaleResults())
						 .Select(x => new Projection { UserName = x.Name })
						 .Lazily();

					s.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

					Assert.Equal("Test User", query.Value.First().UserName);
				}
			}
		}

		private class Projection
		{
			public string UserName { get; set; }
		}

		private class User
		{
			public string Name { get; set; }
		}
	}
}