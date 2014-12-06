// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2627.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using Raven.Abstractions.Data;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2627 : RavenTest
	{
		[Fact]
		public void CanCreateSubscription()
		{
			using (var store = NewDocumentStore())
			{
				store.Subscriptions.Create("OrdersSubscription", new SubscriptionCriteria(), new SubscriptionBatchOptions());
			}
		}

		[Fact]
		public void ShouldStreamAllDocuments()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Age = 31}, "users/1");
					session.Store(new User { Age = 27}, "users/12");
					session.Store(new User { Age = 25}, "users/3");

					session.SaveChanges();
				}

				var subscription = store.Subscriptions.Create("OrdersSubscription", new SubscriptionCriteria(), new SubscriptionBatchOptions());

				var keys = new BlockingCollection<string>();
				var ages = new BlockingCollection<int>();

				subscription.Subscribe(x => keys.Add(x.Key));

				subscription.Subscribe(x => ages.Add(x.DataAsJson.Value<int>("Age")));

				//subscription.Task.Wait(); // TODO arek

				string key;
				Assert.True(keys.TryTake(out key, TimeSpan.FromSeconds(10)));
				Assert.Equal("users/1", key);

				Assert.True(keys.TryTake(out key, TimeSpan.FromSeconds(10)));
				Assert.Equal("users/12", key);

				Assert.True(keys.TryTake(out key, TimeSpan.FromSeconds(10)));
				Assert.Equal("users/3", key);

				int age;
				Assert.True(ages.TryTake(out age, TimeSpan.FromSeconds(10)));
				Assert.Equal(31, age);

				Assert.True(ages.TryTake(out age, TimeSpan.FromSeconds(10)));
				Assert.Equal(27, age);

				Assert.True(ages.TryTake(out age, TimeSpan.FromSeconds(10)));
				Assert.Equal(25, age);
			}
		}

		[Fact(Skip = "Do not push empty patches from server")]
		public void ShouldSendAllNewAndModifiedDocs()
		{
			using (var store = NewDocumentStore())
			{
				var subscription = store.Subscriptions.Create("OrdersSubscription", new SubscriptionCriteria(), new SubscriptionBatchOptions());

				var names = new BlockingCollection<string>();

				subscription.Subscribe(x => names.Add(x.DataAsJson.Value<string>("Name")));

				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "James" }, "users/1");
					session.SaveChanges();
				}

				string name;
				Assert.True(names.TryTake(out name, TimeSpan.FromSeconds(10)));
				Assert.Equal("James", name);

				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "Adam"}, "users/12");
					session.SaveChanges();
				}

				Assert.True(names.TryTake(out name, TimeSpan.FromSeconds(10)));
				Assert.Equal("Adam", name);

				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "David"}, "users/1");
					session.SaveChanges();
				}

				Assert.True(names.TryTake(out name, TimeSpan.FromSeconds(10)));
				Assert.Equal("David", name);
			}
		}
	}
}