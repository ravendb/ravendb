//-----------------------------------------------------------------------
// <copyright file="Includes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Client.Document;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Queries
{
	public class Includes : RemoteClientTest
	{
		[Fact]
		public void CanGenerateComplexPaths()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User { Name = "Ayende" });
					s.Store(new User
					{
						Name = "Rahien",
						Friends = new[]
						{
							new DenormalizedReference {Name = "Ayende", Id = "users/1"},
						}
					});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var user = s.Include<User>(x => x.Friends.Select(f => f.Id)).Load<User>("users/2");
					Assert.Equal(1, user.Friends.Length);
					foreach (var denormalizedReference in user.Friends)
					{
						s.Load<User>(denormalizedReference.Id);
					}

					Assert.Equal(1, s.Advanced.NumberOfRequests);
				}
			}
		}

		[Fact]
		public void CanIncludeViaNestedPath()
		{
			using(GetNewServer())
			using(var store = new DocumentStore{Url = "http://localhost:8079"}.Initialize())
			{
				using(var s = store.OpenSession())
				{
					s.Store(new User{Name = "Ayende"});
					s.Store(new User
					{
						Name = "Rahien",
						Friends = new[]
						{
							new DenormalizedReference {Name = "Ayende", Id = "users/1"},
						}
					});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var user = s.Include("Friends,Id").Load<User>("users/2");
					Assert.Equal(1, user.Friends.Length);
					foreach (var denormalizedReference in user.Friends)
					{
						s.Load<User>(denormalizedReference.Id);
					}

					Assert.Equal(1, s.Advanced.NumberOfRequests);
				}
			}
		}

		public class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public DenormalizedReference[] Friends { get; set; }
		}

		public class DenormalizedReference
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
	}
}