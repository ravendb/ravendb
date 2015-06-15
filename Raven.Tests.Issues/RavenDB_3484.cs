// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3484.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Client.Document;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3484 : RavenTest
	{
		private readonly TimeSpan waitForDocTimeout = TimeSpan.FromSeconds(20);

		[Fact]
		public void OpenIfFree_ShouldBeDefaultStrategy()
		{
			Assert.Equal(SubscriptionOpeningStrategy.OpenIfFree, new SubscriptionConnectionOptions().Strategy);
		}

		[Fact]
		public void ShouldRejectWhen_OpenIfFree_StrategyIsUsed()
		{
			using (var store = NewDocumentStore())
			{
				var id = store.Subscriptions.Create(new SubscriptionCriteria());
				store.Subscriptions.Open(id, new SubscriptionConnectionOptions());
				store.Changes().WaitForAllPendingSubscriptions();

				Assert.Throws<SubscriptionInUseException>(() => store.Subscriptions.Open(id, new SubscriptionConnectionOptions()
				{
					Strategy = SubscriptionOpeningStrategy.OpenIfFree
				}));
			}
		}

		[Fact]
		public void ShouldReplaceActiveClientWhen_TakeOver_StrategyIsUsed()
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
						Strategy = SubscriptionOpeningStrategy.TakeOver
					});

					store.Changes().WaitForAllPendingSubscriptions();

					items[i] = new BlockingCollection<User>();

					subscriptions[i].Subscribe(items[i].Add);

					bool batchAcknowledged = false;

					subscriptions[i].AfterBatch += () => batchAcknowledged = true;

					using (var s = store.OpenSession())
					{
						s.Store(new User());
						s.Store(new User());
						
						s.SaveChanges();
					}
					
					User user;

					Assert.True(items[i].TryTake(out user, waitForDocTimeout));
					Assert.True(items[i].TryTake(out user, waitForDocTimeout));

					SpinWait.SpinUntil(() => batchAcknowledged, TimeSpan.FromSeconds(5)); // let it acknowledge the processed batch before we open another subscription

					if (i > 0)
					{
						Assert.False(items[i - 1].TryTake(out user, TimeSpan.FromSeconds(2)));
						Assert.True(SpinWait.SpinUntil(() => subscriptions[i - 1].IsConnectionClosed, TimeSpan.FromSeconds(5)));
						Assert.True(subscriptions[i - 1].SubscriptionConnectionException is SubscriptionInUseException);
					}
				}
			}
		}

		[Fact]
		public void ShouldReplaceActiveClientWhen_Force_StrategyIsUsed()
		{
			using (var store = NewDocumentStore())
			{
				foreach (var strategyToReplace in new []{SubscriptionOpeningStrategy.OpenIfFree, SubscriptionOpeningStrategy.TakeOver})
				{
					var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());
					var subscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
					{
						Strategy = strategyToReplace
					});

					var items = new BlockingCollection<User>();

					subscription.Subscribe(items.Add);

					var forcedSubscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
					{
						Strategy = SubscriptionOpeningStrategy.Force
					});

					store.Changes().WaitForAllPendingSubscriptions();

					var forcedItems = new BlockingCollection<User>();

					forcedSubscription.Subscribe(forcedItems.Add);

					using (var s = store.OpenSession())
					{
						s.Store(new User());
						s.Store(new User());

						s.SaveChanges();
					}

					User user;

					Assert.True(forcedItems.TryTake(out user, waitForDocTimeout));
					Assert.True(forcedItems.TryTake(out user, waitForDocTimeout));

					Assert.True(SpinWait.SpinUntil(() => subscription.IsConnectionClosed, TimeSpan.FromSeconds(5)));
					Assert.True(subscription.SubscriptionConnectionException is SubscriptionInUseException);
				}
			}
		}

		[Fact]
		public void OpenIfFree_And_TakeOver_StrategiesCannotDropClientWith_Force_Strategy()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());

				var forcedSubscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
				{
					Strategy = SubscriptionOpeningStrategy.Force
				});

				foreach (var strategy in new[] { SubscriptionOpeningStrategy.OpenIfFree, SubscriptionOpeningStrategy.TakeOver })
				{
					Assert.Throws<SubscriptionInUseException>(() => store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
					{
						Strategy = strategy
					}));
				}
			}
		}

		[Fact]
		public void Force_StrategyUsageCanTakeOverAnotherClientWith_Force_Strategy()
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
						Strategy = SubscriptionOpeningStrategy.Force
					});

					store.Changes().WaitForAllPendingSubscriptions();

					items[i] = new BlockingCollection<User>();

					subscriptions[i].Subscribe(items[i].Add);

					bool batchAcknowledged = false;

					subscriptions[i].AfterBatch += () => batchAcknowledged = true;

					using (var s = store.OpenSession())
					{
						s.Store(new User());
						s.Store(new User());

						s.SaveChanges();
					}

					User user;

					Assert.True(items[i].TryTake(out user, waitForDocTimeout));
					Assert.True(items[i].TryTake(out user, waitForDocTimeout));

					SpinWait.SpinUntil(() => batchAcknowledged, TimeSpan.FromSeconds(5)); // let it acknowledge the processed batch before we open another subscription

					if (i > 0)
					{
						Assert.False(items[i - 1].TryTake(out user, TimeSpan.FromSeconds(2)));
						Assert.True(SpinWait.SpinUntil(() => subscriptions[i - 1].IsConnectionClosed, TimeSpan.FromSeconds(5)));
						Assert.True(subscriptions[i - 1].SubscriptionConnectionException is SubscriptionInUseException);
					}
				}
			}
		}
	}
}