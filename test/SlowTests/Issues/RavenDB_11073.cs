using System;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11073 : RavenTestBase
    {
        public RavenDB_11073(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GettingAttachmentNamesForUnstoredEntityShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<ArgumentException>(() => session.Advanced.Attachments.GetNames(new { }));
                    Assert.Contains("is not associated with the session. You need to track the entity in the session.", e.Message);
                }
            }
        }
    }
}
