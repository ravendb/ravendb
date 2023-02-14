using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19598 : RavenTestBase
    {
        public RavenDB_19598(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TestRefreshOverload()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    // creating documents and store them
                    SimpleDoc[] docs = new SimpleDoc[3];
                    docs[0] = new SimpleDoc() { Id = "TestDoc0", Name = "State0" };
                    docs[1] = new SimpleDoc() { Id = "TestDoc1", Name = "State1" };
                    docs[2] = new SimpleDoc() { Id = "TestDoc2", Name = "State2" };

                    foreach (var doc in docs)
                    {
                        session.Store(doc);
                    }
                    session.SaveChanges();

                    // loading the stored docs and name field equality assertions
                    SimpleDoc sd = session.Load<SimpleDoc>(docs[0].Id);
                    Assert.Equal(docs[0].Name, sd.Name);

                    SimpleDoc sd1 = session.Load<SimpleDoc>(docs[1].Id);
                    Assert.Equal(docs[1].Name, sd1.Name);

                    SimpleDoc sd2 = session.Load<SimpleDoc>(docs[2].Id);
                    Assert.Equal(docs[2].Name, sd2.Name);

                    // changing the name fields and saving the changes
                    sd.Name = "grossesNashorn";
                    sd1.Name = "kleinesNashorn";
                    sd2.Name = "krassesNashorn";
                    session.SaveChanges();

                    // overriding locally the name fields (without saving)
                    sd.Name = "schwarzeKraehe";
                    sd1.Name = "weisseKraehe";
                    sd2.Name = "gelbeKraehe";

                    session.Advanced.Refresh(new[]{sd,sd1,sd2}.AsEnumerable());

                    /*
                    session.Advanced.Refresh(new[] { sd, sd, sd }.AsEnumerable());
                    Assert.Equal("grossesNashorn", sd.Name);
                    */
                  
                    // equality assertion of current names and pre-override names
                    Assert.Equal("grossesNashorn", sd.Name);
                    Assert.Equal("kleinesNashorn", sd1.Name);
                    Assert.Equal("krassesNashorn", sd2.Name);
                }
            }
        }

        [Fact]
        public async Task TestRefreshOverloadAsync()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {  // creating documents and store them
                    SimpleDoc[] docs = new SimpleDoc[3];
                    docs[0] = new SimpleDoc() { Id = "TestDoc0", Name = "State0" };
                    docs[1] = new SimpleDoc() { Id = "TestDoc1", Name = "State1" };
                    docs[2] = new SimpleDoc() { Id = "TestDoc2", Name = "State2" };

                    foreach (var doc in docs)
                    {
                        await session.StoreAsync(doc);
                    }
                    await session.SaveChangesAsync();

                    // loading the stored docs and name field equality assertions
                    SimpleDoc sd = await session.LoadAsync<SimpleDoc>(docs[0].Id);
                    Assert.Equal(docs[0].Name, sd.Name);

                    SimpleDoc sd1 = await session.LoadAsync<SimpleDoc>(docs[1].Id);
                    Assert.Equal(docs[1].Name, sd1.Name);

                    SimpleDoc sd2 = await session.LoadAsync<SimpleDoc>(docs[2].Id);
                    Assert.Equal(docs[2].Name, sd2.Name);

                    // changing the name fields and saving the changes
                    sd.Name = "grossesNashorn";
                    sd1.Name = "kleinesNashorn";
                    sd2.Name = "krassesNashorn";
                    await session.SaveChangesAsync();
                    
                    // overriding locally the name fields (without saving)
                    sd.Name = "schwarzeKraehe";
                    sd1.Name = "weisseKraehe";
                    sd2.Name = "gelbeKraehe";

                    await session.Advanced.RefreshAsync(new[] { sd, sd1, sd2 }.AsEnumerable());

                    /*
                    await session.Advanced.RefreshAsync(new[] { sd, sd, sd }.AsEnumerable());
                    Assert.Equal("grossesNashorn", sd.Name);
                    */

                    // equality assertion of current names and pre-override names
                    Assert.Equal("grossesNashorn", sd.Name);
                    Assert.Equal("kleinesNashorn", sd1.Name);
                    Assert.Equal("krassesNashorn", sd2.Name);
                }
            }
        }

        private class SimpleDoc
        {
            public string Id { get; set; }
            public string Name { get; set; }

        }

    }
}
