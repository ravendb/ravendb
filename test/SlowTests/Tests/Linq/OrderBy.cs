using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Tests.Linq
{
    public class OrderBy : RavenTestBase
    {
        public OrderBy(ITestOutputHelper output) : base(output)
        {
        }

        private class Section
        {
            public string Id { get; set; }
            public int Position { get; set; }
            public string Name { get; set; }

            public Section(int position)
            {
                Position = position;
                Name = string.Format("Position: {0}", position);
            }
        }

        [Theory]
        [RavenData]
        public void CanDescOrderBy_AProjection(Options config)
        {
            using (var store = GetDocumentStore(config))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                        session.Store(new Section(i));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var lastPosition = session.Query<Section>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .OrderByDescending(x => x.Position)
                        .Select(x => x.Position)
                        .FirstOrDefault();

                    Assert.Equal(9, lastPosition);
                }
            }
        }

        [Theory]
        [RavenData]
        public void CanAscOrderBy_AProjection(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 5; i < 10; i++)
                        session.Store(new Section(i));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 4; i >= 0; i--)
                        session.Store(new Section(i));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var lastPosition = session.Query<Section>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .OrderBy(x => x.Position)
                        .Select(x => x.Position)
                        .FirstOrDefault();

                    Assert.Equal(0, lastPosition);
                }
            }
        }
    }
}
