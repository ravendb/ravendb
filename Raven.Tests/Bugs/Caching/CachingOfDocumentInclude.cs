//-----------------------------------------------------------------------
// <copyright file="CachingOfDocumentInclude.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Caching
{
	public class CachingOfDocumentInclude : RemoteClientTest
	{
		[Fact]
		public void Can_cache_document_with_includes()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User {Name = "Ayende"});
					s.Store(new User { PartnerId = "users/1"});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					s.Include<User>(x=>x.PartnerId)
						.Load("users/2");
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					s.Include<User>(x => x.PartnerId)
						.Load("users/2");
					Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
				}
			}
		}

		[Fact]
		public void Will_referesh_result_when_main_document_changes()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Ayende" });
					s.Store(new User { PartnerId = "users/1" });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					s.Include<User>(x => x.PartnerId)
						.Load("users/2");
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var user = s.Include<User>(x => x.PartnerId)
						.Load("users/2");
					Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
					user.Name = "Foo";
					s.SaveChanges();
				}


				using (var s = store.OpenSession())
				{
					s.Include<User>(x => x.PartnerId)
						.Load("users/2");
					Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests); // did NOT increase cache
				}
			}
		}
		
		[Fact]
		public void New_query_returns_correct_value_when_cache_is_enabled_and_data_changes ()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Ayende", Email="same.email@example.com"});
					store.DatabaseCommands.PutIndex("index",
														 new IndexDefinition()
															 {
																 Map =
																	 "from user in docs.Users select new {Email=user.Email}"
															 });
					s.SaveChanges();
				}

				DateTime firstTime = SystemTime.Now;

				using (var s = store.OpenSession())
				{
					var results = s.Query<User>("index")
						.Customize(q => q.WaitForNonStaleResultsAsOf(firstTime))
						.Where(u => u.Email == "same.email@example.com")
						.ToArray();
					// Cache is done by url, so including a cutoff date invalidates the cache.

					// the second query should stay in cache and return the correct value
					results = s.Query<User>("index")
						.Where(u => u.Email == "same.email@example.com")
						.ToArray();
					Assert.Equal(1, results.Length);
				}

				DateTime secondTime = SystemTime.Now;

				if (firstTime == secondTime) // avoid getting the exact same url
					secondTime = secondTime.AddMilliseconds(100);

				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Other", Email = "same.email@example.com" });
					s.SaveChanges();
				}


				using (var s = store.OpenSession())
				{
					var results = s.Query<User>("index")
						.Customize(q => q.WaitForNonStaleResultsAsOf(secondTime))
						.Where(u => u.Email == "same.email@example.com")
						.ToArray();
					// this works, since we don't hit the cache
					Assert.Equal(2, results.Length);

					// we now hit the cache, but it should be invalidated since the underlying index *has* changed
					// it isn't invalidated, and the result returns just 1 result
					results = s.Query<User>("index")
						.Where(u => u.Email == "same.email@example.com")
						.ToArray();
					Assert.Equal(2, results.Length);
				}
			}
		}

		[Fact]
		public void Will_referesh_result_when_included_document_changes()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Ayende" });
					s.Store(new User { PartnerId = "users/1" });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					s.Include<User>(x => x.PartnerId)
						.Load("users/2");
				}

				using (var s = store.OpenSession())
				{
					s.Include<User>(x => x.PartnerId)
						.Load("users/2");
					Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
					s.Load<User>("users/1").Name = "foo";
					s.SaveChanges();
				}


				using (var s = store.OpenSession())
				{
					s.Include<User>(x => x.PartnerId)
						.Load("users/2");
					Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests); // did NOT increase cache
				}
			}
		}
	}
}