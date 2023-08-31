using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14176 : RavenTestBase
    {
        public RavenDB_14176(ITestOutputHelper output) : base(output)
        {
        }

        private class Doc
        {
            public string DocumentType { get; set; }
            public string Status { get; set; }
            public string CreatedBy { get; set; }
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Doc { DocumentType = "Test", Status = "draft", CreatedBy = "user1" }, "doc/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Advanced.RawQuery<Doc>(@"
                            from Docs
                            where DocumentType = 'Test'
                            and Status = 'draft'
                            and search(CreatedBy, 'user*') and CreatedBy = 'user1'");

                    var r = q.ToList();
                    Assert.Equal(1, r.Count);
                }
            }
        }

        [Fact]
        public void ShouldWorkAndWorks()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Doc { DocumentType = "Test", Status = "draft", CreatedBy = "user1" }, "doc/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Advanced.RawQuery<Doc>(@"
                            from Docs
                            where DocumentType = 'Test'
                            and Status = 'draft'
                            and search(CreatedBy, 'user*')");

                    var r = q.ToList();
                    Assert.Equal(1, r.Count);

                    // debug assert fail but return the right result
                    var q2 = session.Advanced.RawQuery<Doc>(@"
                            from Docs
                            where DocumentType = 'Test'
                            and Status = 'draft'
                            and search(CreatedBy, 'user*') and CreatedBy = 'user1'");

                    var r2 = q2.ToList();
                    Assert.Equal(1, r2.Count);
                }
            }
        }
    }
}
