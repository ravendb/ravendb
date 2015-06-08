// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3484.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3484 : RavenTest
	{
		private readonly TimeSpan waitForDocTimeout = TimeSpan.FromSeconds(20);

		[Fact]
		public void FirstKeepsOpen_ShouldBeDefaultStrategy()
		{
			Assert.Equal(SubscriptionOpeningStrategy.FirstKeepsOpen, new SubscriptionConnectionOptions().Strategy);
		}

		[Fact]
		public void ShouldRejectWhen_FirstKeepsOpen_StrategyIsUsed()
		{
			using (var store = NewDocumentStore())
			{
				var id = store.Subscriptions.Create(new SubscriptionCriteria());
				store.Subscriptions.Open(id, new SubscriptionConnectionOptions());
				store.Changes().WaitForAllPendingSubscriptions();

				Assert.Throws<SubscriptionInUseException>(() => store.Subscriptions.Open(id, new SubscriptionConnectionOptions()
				{
					Strategy = SubscriptionOpeningStrategy.FirstKeepsOpen
				}));
			}
		}

		[Fact]
		public void ShouldReplaceActiveClientWhen_LastTakesOver_StrategyIsUsed()
		{
			using (var store = NewDocumentStore())
			{
				var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());
				
				const int numberOfClients = 4;

				var subscriptions = new Subscription<User>[numberOfClients];
				var items = new BlockingCollection<User>[numberOfClients];

				for (int i = 0; i < numberOfClients; i++)
				{
					subscriptions[i] = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
					{
						Strategy = SubscriptionOpeningStrategy.LastTakesOver
					});

					store.Changes().WaitForAllPendingSubscriptions();

					items[i] = new BlockingCollection<User>();

					subscriptions[i].Subscribe(items[i].Add);

					using (var s = store.OpenSession())
					{
						s.Store(new User());
						s.Store(new User());
						
						s.SaveChanges();
					}

					User user;

					Assert.True(items[i].TryTake(out user, waitForDocTimeout));
					Assert.True(items[i].TryTake(out user, waitForDocTimeout));

					if (i > 0)
					{
						Assert.False(items[i - 1].TryTake(out user, waitForDocTimeout));
						Assert.True(subscriptions[i - 1].SubscriptionConnectionException is SubscriptionInUseException);
						Assert.True(subscriptions[i - 1].IsClosed);
					}
				}
			}
		}

		[Fact]
		public void ShouldAddToQueueWhen_QueueIn_StrategyIsUsed()
		{
			using (var store = NewDocumentStore())
			{
				var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());
				var subscription1 = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions());

				var items1 = new BlockingCollection<User>();

				subscription1.Subscribe(items1.Add);

				store.Changes().WaitForAllPendingSubscriptions();

				var subscription2 = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
				{
					Strategy = SubscriptionOpeningStrategy.QueueIn
				});

				store.Changes().WaitForAllPendingSubscriptions();

				var items2 = new BlockingCollection<User>();

				subscription2.Subscribe(items2.Add);

				using (var s = store.OpenSession())
				{
					s.Store(new User(), "users/1");
					s.Store(new User(), "users/2");
					s.SaveChanges();
				}

				User user;

				Assert.True(items1.TryTake(out user, waitForDocTimeout));
				Assert.Equal("users/1", user.Id);
				Assert.True(items1.TryTake(out user, waitForDocTimeout));
				Assert.Equal("users/2", user.Id);

				subscription1.Dispose(); // release the connected subscription

				using (var s = store.OpenSession())
				{
					s.Store(new User(), "users/3");
					s.Store(new User(), "users/4");
					s.SaveChanges();
				}

				Assert.True(items2.TryTake(out user, waitForDocTimeout));
				Assert.Equal("users/3", user.Id);
				Assert.True(items2.TryTake(out user, waitForDocTimeout));
				Assert.Equal("users/4", user.Id);
			}
		}
	}
}