using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace FastTests.Client.Documents
{
    public class Load:RavenTestBase
    {
        public class Foo
        {
            public string Name { get; set; }
        }

        public class Bar
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
            public object FooId { get; set; }
            public List<object> FooIDs { get; set; }
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
                    var foo = new Foo {Name="Beginning"};
                    session.Store(foo);
                    var fooId = barId = session.Advanced.GetDocumentId(foo);
                    var bar = new Bar
                    {
                        Name="End",
                        FooId = fooId
                    };
                    session.Store(bar);
                    barId = session.Advanced.GetDocumentId(bar);
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewSession())
                {
                    var bar  =  newSession.LoadInternal<Bar>(new[] {barId},new []
                    {
                        new KeyValuePair<string, Type>("FooId",typeof(string))
                    });
                    Assert.NotNull(bar);
                    Assert.Equal(1,bar.Length);
                    Assert.NotNull(bar[0]);

                    var numOfRequests = newSession.NumberOfRequests;

                    var foo = newSession.Load<Foo>((string)bar[0].FooId);

                    Assert.NotNull(foo);
                    Assert.Equal("Beginning",foo.Name);
                    Assert.Equal(numOfRequests, newSession.NumberOfRequests);
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

                using (var newSession = store.OpenNewSession())
                {
                    var bar = newSession.LoadInternal<Bar>(new[] { barId }, new[]
                    {
                        new KeyValuePair<string, Type>("FooId",typeof(string))
                    });
                    Assert.NotNull(bar);
                    Assert.Equal(1, bar.Length);
                    Assert.NotNull(bar[0]);

                    var numOfRequests = newSession.NumberOfRequests;

                    var foo = newSession.Load<Foo>((string)bar[0].FooId);

                    Assert.Null(foo);
                    Assert.Equal(numOfRequests, newSession.NumberOfRequests);
                }

            }
        }

    }
}
