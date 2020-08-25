using System.IO;
using System.Text;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15521 : RavenTestBase
    {
        public RavenDB_15521(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    SimpleDoc doc = new SimpleDoc() { Id = "TestDoc", Name = "State1" };
                    session.Store(doc);

                    string attachment = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
                    var stream = new MemoryStream(Encoding.UTF8.GetBytes(attachment));
                    session.Advanced.Attachments.Store(doc, "TestAttachment", stream);

                    session.SaveChanges();
                    var changeVector1 = session.Advanced.GetChangeVectorFor(doc);
                    session.Advanced.Refresh(doc);
                    var changeVector2 = session.Advanced.GetChangeVectorFor(doc);
                    Assert.Equal(changeVector1, changeVector2);
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
