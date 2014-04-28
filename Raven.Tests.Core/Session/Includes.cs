// -----------------------------------------------------------------------
//  <copyright file="Includes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Core.Utils.Entities;

using Xunit;

namespace Raven.Tests.Core.Session
{
	public class Includes : RavenCoreTestBase
	{
		[Fact]
		public void BasicInclude()
		{
			using (var store = GetDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var user = new User
					           {
						           Id = "users/1",
								   AddressId = "addresses/1",
								   Name = "John"
					           };

					var address = new Address
					              {
						              Id = "addresses/1",
									  City = "New York",
									  Country = "USA",
									  Street = "Wall Street"
					              };

					session.Store(user);
					session.Store(address);

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.Equal(0, session.Advanced.NumberOfRequests);

					var user = session
						.Include<User>(x => x.AddressId)
						.Load<User>("users/1");

					Assert.Equal(1, session.Advanced.NumberOfRequests);

					Assert.NotNull(user);
					Assert.Equal("John", user.Name);

					var address = session.Load<Address>(user.AddressId);

					Assert.Equal(1, session.Advanced.NumberOfRequests);

					Assert.Equal("New York", address.City);
				}
			}
		}
	}
}