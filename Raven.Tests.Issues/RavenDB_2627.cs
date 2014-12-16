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
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2627 : RavenTest
	{
		private readonly TimeSpan waitForDocTimeout = TimeSpan.FromSeconds(20);

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
		public void ShouldStreamAllDocumentsAfterSubscriptionCreation()
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
				Assert.True(keys.TryTake(out key, waitForDocTimeout));
				Assert.Equal("users/1", key);

				Assert.True(keys.TryTake(out key, waitForDocTimeout));
				Assert.Equal("users/12", key);

				Assert.True(keys.TryTake(out key, waitForDocTimeout));
				Assert.Equal("users/3", key);

				int age;
				Assert.True(ages.TryTake(out age, waitForDocTimeout));
				Assert.Equal(31, age);

				Assert.True(ages.TryTake(out age, waitForDocTimeout));
				Assert.Equal(27, age);

				Assert.True(ages.TryTake(out age, waitForDocTimeout));
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
				Assert.True(names.TryTake(out name, waitForDocTimeout));
				Assert.Equal("James", name);

				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "Adam"}, "users/12");
					session.SaveChanges();
				}

				Assert.True(names.TryTake(out name, waitForDocTimeout));
				Assert.Equal("Adam", name);

				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "David"}, "users/1");
					session.SaveChanges();
				}

				Assert.True(names.TryTake(out name, waitForDocTimeout));
				Assert.Equal("David", name);
			}
		}

		[Fact]
		public void ShouldResendDocsIfAcknowledgmentTimeoutOccurred()
		{
			using (var store = NewDocumentStore())
			{
				var id = store.Subscriptions.Create(new SubscriptionCriteria());
				var subscriptionZeroTimeout = store.Subscriptions.Open(id, new SubscriptionBatchOptions
				{
					AcknowledgmentTimeout = TimeSpan.FromSeconds(0) // the client won't be able to acknowledge in 0 seconds
				});

				var docs = new BlockingCollection<JsonDocument>();

				subscriptionZeroTimeout.Subscribe(docs.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new User {Name = "Raven"});
					session.SaveChanges();
				}

				JsonDocument document;

				Assert.True(docs.TryTake(out document, waitForDocTimeout));
				Assert.Equal("Raven", document.DataAsJson.Value<string>("Name"));

				Assert.True(docs.TryTake(out document, waitForDocTimeout));
				Assert.Equal("Raven", document.DataAsJson.Value<string>("Name"));

				Assert.True(docs.TryTake(out document, waitForDocTimeout));
				Assert.Equal("Raven", document.DataAsJson.Value<string>("Name"));

				subscriptionZeroTimeout.Dispose();

				// retry with longer timeouts - should sent just one doc

				var subscriptionLongerTimeout = store.Subscriptions.Open(id, new SubscriptionBatchOptions
				{
					AcknowledgmentTimeout = TimeSpan.FromSeconds(30)
				});

				var docs2 = new BlockingCollection<JsonDocument>();

				subscriptionLongerTimeout.Subscribe(docs2.Add);

				Assert.True(docs2.TryTake(out document, waitForDocTimeout));
				Assert.Equal("Raven", document.DataAsJson.Value<string>("Name"));

				Assert.False(docs2.TryTake(out document, waitForDocTimeout));
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
						session.Store(new Company());
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

		[Fact]
		public void ShouldRespectMaxBatchSize()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 100; i++)
					{
						session.Store(new Company());
						session.Store(new User());
					}

					session.SaveChanges();
				}

				var id = store.Subscriptions.Create(new SubscriptionCriteria());
				var subscription = store.Subscriptions.Open(id, new SubscriptionBatchOptions()
				{
					MaxSize = 16 * 1024
				});

				var batches = new List<List<JsonDocument>>();

				subscription.BeforeBatch +=
					() => batches.Add(new List<JsonDocument>());

				subscription.Subscribe(x =>
				{
					var list = batches.Last();
					list.Add(x);
				});

				var result = SpinWait.SpinUntil(() => batches.Sum(x => x.Count) >= 200, TimeSpan.FromSeconds(160));

				Assert.True(result);
				Assert.True(batches.Count > 1);
			}
		}

		[Fact]
		public void ShouldRespectCollectionCriteria()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 100; i++)
					{
						session.Store(new Company());
						session.Store(new User());
					}

					session.SaveChanges();
				}

				var id = store.Subscriptions.Create(new SubscriptionCriteria
				{
					BelongsToCollection = "Users"
				});

				var subscription = store.Subscriptions.Open(id, new SubscriptionBatchOptions { MaxDocCount = 31 });

				var docs = new List<JsonDocument>();

				subscription.Subscribe(docs.Add);

				Assert.True(SpinWait.SpinUntil(() => docs.Count >= 100, TimeSpan.FromSeconds(60)));


				foreach (var jsonDocument in docs)
				{
					Assert.Equal("Users", jsonDocument.Metadata.Value<string>(Constants.RavenEntityName));
				}
			}
		}

		[Fact]
		public void ShouldRespectStartsWithCriteria()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 100; i++)
					{
						session.Store(new User(), i % 2 == 0 ? "users/" : "users/favorite/");
					}

					session.SaveChanges();
				}

				var id = store.Subscriptions.Create(new SubscriptionCriteria
				{
					KeyStartsWith = "users/favorite/"
				});

				var subscription = store.Subscriptions.Open(id, new SubscriptionBatchOptions { MaxDocCount = 15 });

				var docs = new List<JsonDocument>();

				subscription.Subscribe(docs.Add);

				Assert.True(SpinWait.SpinUntil(() => docs.Count >= 50, TimeSpan.FromSeconds(60)));


				foreach (var jsonDocument in docs)
				{
					Assert.True(jsonDocument.Key.StartsWith("users/favorite/"));
				}
			}
		}

		[Fact]
		public void ShouldRespectPropertiesCriteria()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						session.Store(new User
						{
							Name = i % 2 == 0 ? "Jessica" : "Caroline"
						});

						session.Store(new Person
						{
							Name = i % 2 == 0 ? "Caroline" : "Samantha"
						});

						session.Store(new Company());
					}

					session.SaveChanges();
				}

				var id = store.Subscriptions.Create(new SubscriptionCriteria
				{
					PropertiesMatch = new Dictionary<string, RavenJToken>()
					{
						{"Name", "Caroline"}
					}
				});

				var carolines = store.Subscriptions.Open(id, new SubscriptionBatchOptions { MaxDocCount = 5 });

				var docs = new List<JsonDocument>();

				carolines.Subscribe(docs.Add);

				Assert.True(SpinWait.SpinUntil(() => docs.Count >= 10, TimeSpan.FromSeconds(60)));


				foreach (var jsonDocument in docs)
				{
					Assert.Equal("Caroline", jsonDocument.DataAsJson.Value<string>("Name"));
				}
			}
		}

		[Fact]
		public void ShouldRespectPropertiesNotMatchCriteria()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						session.Store(new User
						{
							Name = i % 2 == 0 ? "Jessica" : "Caroline"
						});

						session.Store(new Person
						{
							Name = i % 2 == 0 ? "Caroline" : "Samantha"
						});

						session.Store(new Company());
					}

					session.SaveChanges();
				}

				var id = store.Subscriptions.Create(new SubscriptionCriteria
				{
					PropertiesNotMatch = new Dictionary<string, RavenJToken>()
					{
						{"Name", "Caroline"}
					}
				});

				var subscription = store.Subscriptions.Open(id, new SubscriptionBatchOptions { MaxDocCount = 5 });

				var docs = new List<JsonDocument>();

				subscription.Subscribe(docs.Add);

				Assert.True(SpinWait.SpinUntil(() => docs.Count >= 20, TimeSpan.FromSeconds(60)));


				foreach (var jsonDocument in docs)
				{
					Assert.True(jsonDocument.DataAsJson.ContainsKey("Name") == false || jsonDocument.DataAsJson.Value<string>("Name") != "Caroline");
				}
			}
		}
	}
}