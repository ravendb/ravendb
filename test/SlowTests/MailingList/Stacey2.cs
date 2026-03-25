using System.Collections.Generic;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.MailingList
{
    public class Stacey2 : RavenTestBase
    {
        public Stacey2(ITestOutputHelper output) : base(output)
        {
        }

        private class Root
        {
            public string Id { get; set; }
            public Bridge Bridge { get; set; }
        }
        private class Bridge
        {
            public List<string> Aggregates { get; set; }
        }
        private class Aggregate
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void LoadWithInclude()
        {
            using (var store = GetDocumentStore())
            {

                var aggregate = new Aggregate
                {
                    Name = "First"
                };

                using (var session = store.OpenSession())
                {
#pragma warning disable CS0618 // Type or member is obsolete
                    session.Advanced.UseOptimisticConcurrency = true;
#pragma warning restore CS0618 // Type or member is obsolete
                    session.Store(aggregate);
                    session.SaveChanges();
                }

                var root = new Root
                {
                    Bridge = new Bridge
                    {
                        Aggregates = new List<string>
                        {
                            aggregate.Id
                        }
                    }
                };

                using (var session = store.OpenSession())
                {
#pragma warning disable CS0618 // Type or member is obsolete
                    session.Advanced.UseOptimisticConcurrency = true;
#pragma warning restore CS0618 // Type or member is obsolete
                    session.Store(root); session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var item = session.Load<Aggregate>("aggregates/1-A");
                    Assert.NotNull(item);
                }

                using (var session = store.OpenSession())
                {
#pragma warning disable CS0618 // Type or member is obsolete
                    session.Advanced.UseOptimisticConcurrency = true;
#pragma warning restore CS0618 // Type or member is obsolete
                    var query = session
                        .Include("Bridge.Aggregates")
                        .Load<Root>("roots/1-A");

                    Assert.NotNull(query);
                }

                using (var session = store.OpenSession())
                {
#pragma warning disable CS0618 // Type or member is obsolete
                    session.Advanced.UseOptimisticConcurrency = true;
#pragma warning restore CS0618 // Type or member is obsolete
                    var query = session
                        .Include("Bridge.Aggregates")
                        .Load<Root>("roots/1-A");
                    var loaded = session.Load<Aggregate>("aggregates/1-A");

                    Assert.NotNull(query);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }

                using (var session = store.OpenSession())
                {
#pragma warning disable CS0618 // Type or member is obsolete
                    session.Advanced.UseOptimisticConcurrency = true;
#pragma warning restore CS0618 // Type or member is obsolete
                    var query = session
                        .Include("Bridge.Aggregates")
                        .Load<Root>("roots/1-A");
                    var loaded = session.Load<Aggregate>("aggregates/1-A");

                    Assert.NotNull(query);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
