using System;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11073 : RavenTestBase
    {
        [Fact]
        public void GettingAttachmentNamesForUnstoredEntityShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<ArgumentException>(() => session.Advanced.Attachments.GetNames(new { }));
                    Assert.Contains("is not associated with the session. Use documentId instead or track the entity in the session", e.Message);
                }
            }
        }
    }
}
