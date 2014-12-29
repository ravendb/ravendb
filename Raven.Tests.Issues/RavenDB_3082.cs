// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3082.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3082 : RavenTest
	{
		[Fact]
		public async Task StronglyTypedDataSubscriptions()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						session.Store(new PersonWithAddress()
						{
							Name = "James",
							Address = new Address()
							{
								ZipCode = 12345
							}
						});

						session.Store(new PersonWithAddress()
						{
							Name = "James",
							Address = new Address()
							{
								ZipCode = 54321
							}
						});

						session.Store(new PersonWithAddress()
						{
							Name = "David",
							Address = new Address()
							{
								ZipCode = 12345
							}
						});

						session.Store(new Person());
					}

					session.SaveChanges();
				}

				var criteria = new SubscriptionCriteria<PersonWithAddress>();
				criteria.PropertyMatch(x => x.Name, "James");
				criteria.PropertyNotMatch(x => x.Address.ZipCode, 54321);

				var id = await store.AsyncSubscriptions.CreateAsync(criteria);

				var subscription = await store.AsyncSubscriptions.OpenAsync<PersonWithAddress>(id, new SubscriptionConnectionOptions());

				var users = new List<PersonWithAddress>();

				subscription.Subscribe(users.Add);

				Assert.True(SpinWait.SpinUntil(() => users.Count >= 10, TimeSpan.FromSeconds(60)));

				Assert.Equal(10, users.Count);

				foreach (var user in users)
				{
					Assert.Equal("James", user.Name);
					Assert.Equal(12345, user.Address.ZipCode);
				}
			}
		}
	}
}