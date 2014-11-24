//-----------------------------------------------------------------------
// <copyright file="CachingOfDocumentLoad.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.Caching
{
	public class CachingOfDocumentLoad : RavenTest
	{
		[Fact]
		public void Can_cache_document_load()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Ayende" });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					s.Load<User>("users/1");
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					s.Load<User>("users/1");
					Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
				}
			}
		}

		[Fact]
		public void Can_NOT_cache_document_load()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				store.Conventions.ShouldCacheRequest = s => false;

				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Ayende" });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					s.Load<User>("users/1");
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					s.Load<User>("users/1");
					Assert.Equal(0, store.JsonRequestFactory.NumberOfCachedRequests);
				}
			}
		}


		[Fact]
		public void After_modification_will_get_value_from_server()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Ayende" });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					s.Load<User>("users/1");
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var user = s.Load<User>("users/1");
					user.Name = "Rahien";
					Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					s.Load<User>("users/1");
					Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests); // did NOT get from cache
				}
			}
		}
	}
}