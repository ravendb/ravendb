using System.IO;
using System.Linq;
using System.Text;
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
                    SimpleDoc[] docs = new SimpleDoc[3];
                    docs[0] = new SimpleDoc() { Id = "TestDoc0", Name = "State0" };
                    docs[1] = new SimpleDoc() { Id = "TestDoc1", Name = "State1" };
                    docs[2] = new SimpleDoc() { Id = "TestDoc2", Name = "State2" };

                    foreach (var doc in docs)
                    {
                        session.Store(doc);
                    }

                    session.SaveChanges();

                    string[] cvsBeforeRefresh = new string[3];
                    for (int i = 0; i < cvsBeforeRefresh.Length; i++)
                    {
                        cvsBeforeRefresh[i] = session.Advanced.GetChangeVectorFor(docs[i]);
                    }
                    
                    foreach (var doc in docs)
                    {
                        doc.Name = "Nashorn";
                        session.Store(doc);
                    
                    }
                    
                    session.SaveChanges();

                    session.Advanced.Refresh(docs);

                    string[] cvsAfterRefresh = new string[3];
                    for (int i = 0; i < cvsAfterRefresh.Length; i++)
                    {
                        cvsAfterRefresh[i] = session.Advanced.GetChangeVectorFor(docs[i]);
                    }

                    var cvsBeforeAndAfter = cvsBeforeRefresh.Zip(cvsAfterRefresh, (b, a) => new { cvBefore = b, cvAfter = a });
                    foreach (var cvs in cvsBeforeAndAfter)
                    {
                        Assert.NotEqual(cvs.cvBefore, cvs.cvAfter);
                    }
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
