// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2627.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
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
				var id = store.Subscriptions.Create(new SubscriptionCriteria());
				Assert.Equal(1, id);

				id = store.Subscriptions.Create(new SubscriptionCriteria());
				Assert.Equal(2, id);
			}
		}

		[Fact]
		public void ShouldThrowWhenOpeningNoExisingSubscription()
		{
			using (var store = NewDocumentStore())
			{
				var ex = Assert.Throws<InvalidOperationException>(() => store.Subscriptions.Open(1, new SubscriptionBatchOptions()));
				Assert.Equal("Subscription with the specified id does not exist.", ex.Message);
			}
		}

		[Fact]
		public void ShouldThrowOnAttemptToOpenAlreadyOpenedSubscription()
		{
			using (var store = NewDocumentStore())
			{
				var id = store.Subscriptions.Create(new SubscriptionCriteria());
				store.Subscriptions.Open(id, new SubscriptionBatchOptions());

				var ex = Assert.Throws<InvalidOperationException>(() => store.Subscriptions.Open(id, new SubscriptionBatchOptions()));
				Assert.Equal("Subscription is already in use. There can be only a single open subscription connection per subscription.", ex.Message);
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

				var id = store.Subscriptions.Create(new SubscriptionCriteria());
				var subscription = store.Subscriptions.Open(id, new SubscriptionBatchOptions());

				var keys = new BlockingCollection<string>();
				var ages = new BlockingCollection<int>();

				subscription.Subscribe(x => keys.Add(x.Key));
				subscription.Subscribe(x => ages.Add(x.DataAsJson.Value<int>("Age")));

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

		[Fact]
		public void ShouldSendAllNewAndModifiedDocs()
		{
			using (var store = NewDocumentStore())
			{
				var id = store.Subscriptions.Create(new SubscriptionCriteria());
				var subscription = store.Subscriptions.Open(id, new SubscriptionBatchOptions());

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

		[Fact]
		public void ShouldRespectMaxDocCountInBatch()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 100; i++)
					{
						session.Store(new User());
					}

					session.SaveChanges();
				}

				var id = store.Subscriptions.Create(new SubscriptionCriteria());
				var subscription = store.Subscriptions.Open(id, new SubscriptionBatchOptions{ MaxDocCount = 25});

				var batchSizes = new List<Reference<int>>();

				subscription.BeforeBatch +=
					() => batchSizes.Add(new Reference<int>());

				subscription.Subscribe(x =>
				{
					var reference = batchSizes.Last();
					reference.Value++;
				});

				var result = SpinWait.SpinUntil(() => batchSizes.Sum(x => x.Value) >= 100, TimeSpan.FromSeconds(60));

				Assert.True(result);

				Assert.Equal(4, batchSizes.Count);

				foreach (var reference in batchSizes)
				{
					Assert.Equal(25, reference.Value);
				}
			}
		}
	}
}