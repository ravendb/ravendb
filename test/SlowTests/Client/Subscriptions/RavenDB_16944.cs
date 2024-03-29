﻿using System.Linq;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_16944 : RavenTestBase
    {
        public RavenDB_16944(ITestOutputHelper output) : base(output) { }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void SubscriptionIsDisabledWhenCreatedAsDisabled(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var entity = new User
                    {
                        Name = "Ed"
                    };
                    session.Store(entity);
                    session.SaveChanges();
                }

                store.Subscriptions.Create(new SubscriptionCreationOptions() { Name = "10", Query = "From Users", Disabled = true }, store.Database);

                using (var session = store.OpenSession())
                {
                    var subs = store.Subscriptions.GetSubscriptions(0, 10);
                    var item = subs.First(x => x.SubscriptionName == "10");
                    Assert.True(item.Disabled);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void SubscriptionIsEnabledWhenCreatedWithoutPassingDisabledArg(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var entity = new User
                    {
                        Name = "Ed"
                    };
                    session.Store(entity);
                    session.SaveChanges();
                }

                store.Subscriptions.Create(new SubscriptionCreationOptions() { Name = "10", Query = "From Users"}, store.Database);

                using (var session = store.OpenSession())
                {
                    var subs = store.Subscriptions.GetSubscriptions(0, 10);
                    var item = subs.First(x => x.SubscriptionName == "10");
                    Assert.False(item.Disabled);
                }
            }
        }
    }
}
