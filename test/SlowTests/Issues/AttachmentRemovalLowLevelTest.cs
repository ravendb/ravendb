using System.IO;
using FastTests;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class AttachmentRemovalLowLevelTest : RavenTestBase
{
    public AttachmentRemovalLowLevelTest(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Attachments)]
    public void WillRemoveAttachment()
    {
        using var store = GetDocumentStore();
        string docId;
        using (var session = store.OpenSession())
        {
            var product = new Product() { Name = nameof(WillRemoveAttachment) };
            session.Store(product);
            session.SaveChanges();
            
            session.Advanced.Attachments.Store(product.Id, "TOREMOVE", new MemoryStream([1,2,3,4]));
            session.SaveChanges();
            docId = product.Id;
        }

        using (var session = store.OpenSession())
        {
            session.Delete(docId);
            session.SaveChanges();
        }        
    }
}
