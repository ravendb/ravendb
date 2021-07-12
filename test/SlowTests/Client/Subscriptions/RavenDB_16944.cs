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
    public class RavenDB_16944:RavenTestBase
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

                using (store.GetRequestExecutor().ContextPool.AllocateOperationContext(out var context))
                {
                    store.GetRequestExecutor()
                        .Execute(
                            new CreateSubscriptionCommand(store.Conventions,
                                new SubscriptionCreationOptions() { Name = "10", Query = "From Users" }, true, "10"), context);
                }

                using (var session = store.OpenSession())
                {
                    var subs = store.Subscriptions.GetSubscriptions(0, 10);
                    var item = subs.FirstOrDefault(x => x.SubscriptionName == "10" && x.Disabled);
                    Assert.NotNull(item);
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

                using (store.GetRequestExecutor().ContextPool.AllocateOperationContext(out var context))
                {
                    store.GetRequestExecutor()
                        .Execute(
                            new CreateSubscriptionCommand(store.Conventions,
                                new SubscriptionCreationOptions() { Name = "10", Query = "From Users" },"10"), context);
                }

                using (var session = store.OpenSession())
                {
                    var subs = store.Subscriptions.GetSubscriptions(0, 10);
                    var item = subs.FirstOrDefault(x => x.SubscriptionName == "10" && !x.Disabled);
                    Assert.NotNull(item);
                }
            }
        }
    }
}
