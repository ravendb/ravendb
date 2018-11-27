using System.Collections.Generic;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client.Documents
{
    public class Load : RavenTestBase
    {
        private class Foo
        {
            public string Name { get; set; }
        }

        private class Bar
        {
            /*public string FooStringId { get; set; }
            public long FooLongId { get; set; }
            public ulong FooUlongLongId { get; set; }
            public int FooIntId { get; set; }
            public uint FooUIntId { get; set; }
            public short FooShortId { get; set; }
            public ushort FooUShortId { get; set; }
            public byte FooByteId { get; set; }
            */
            public string FooId { get; set; }
            public List<string> FooIDs { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void LoadWithIncludes()
        {
            using (var store = GetDocumentStore())
            {
                string barId;
                using (var session = store.OpenSession())
                {
                    var foo = new Foo { Name = "Beginning" };
                    session.Store(foo);
                    var fooId = barId = session.Advanced.GetDocumentId(foo);
                    var bar = new Bar
                    {
                        Name = "End",
                        FooId = fooId
                    };
                    session.Store(bar);
                    barId = session.Advanced.GetDocumentId(bar);
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var bar = newSession.Include<Bar>(x=>x.FooId).Load(new[] {barId});
                    Assert.NotNull(bar);
                    Assert.Equal(1, bar.Count);
                    Assert.NotNull(bar[barId]);

                    var numOfRequests = newSession.Advanced.NumberOfRequests;

                    var foo = newSession.Load<Foo>((string)bar[barId].FooId);

                    Assert.NotNull(foo);
                    Assert.Equal("Beginning", foo.Name);
                    Assert.Equal(numOfRequests, newSession.Advanced.NumberOfRequests);
                }

            }
        }

        [Fact]
        public void LoadWithIncludesAndMissingDocument()
        {
            using (var store = GetDocumentStore())
            {
                string barId;
                using (var session = store.OpenSession())
                {

                    var bar = new Bar
                    {
                        Name = "End",
                        FooId = "somefoo/1"
                    };
                    session.Store(bar);
                    barId = session.Advanced.GetDocumentId(bar);
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var bar = newSession.Include<Bar>(x => x.FooId).Load(new[] { barId });

                    Assert.NotNull(bar);
                    Assert.Equal(1, bar.Count);
                    Assert.NotNull(bar[barId]);

                    var numOfRequests = newSession.Advanced.NumberOfRequests;

                    var foo = newSession.Load<Foo>((string)bar[barId].FooId);

                    Assert.Null(foo);
                    Assert.Equal(numOfRequests, newSession.Advanced.NumberOfRequests);
                }

            }
        }

    }
}
