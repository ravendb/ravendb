using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_16944 : RavenTestBase
    {
        public RavenDB_16944(ITestOutputHelper output) : base(output) { }

        [Fact]
        public void SubscriptionIsDisabledWhenCreatedAsDisabled()
        {
            using (var store = GetDocumentStore())
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

        [Fact]
        public void SubscriptionIsEnabledWhenCreatedWithoutPassingDisabledArg()
        {
            using (var store = GetDocumentStore())
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
