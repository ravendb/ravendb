using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Adrian : RavenTestBase
    {
        private class ContentDescriptorByMetadata : AbstractIndexCreationTask<ContentDescriptor>
        {
            public ContentDescriptorByMetadata()
            {
                Map = cds =>
                    from cd in cds
                    select new
                    {
                        _ = cd.Identify.Select(x => CreateField(x.Key, x.Value))
                    };
            }
        }

        private class ContentDescriptor
        {
            public Dictionary<string, string> Identify { get; set; }
        }

        [Fact(Skip = "Missing feature: CreateField")]
        public void CanCreateIndex()
        {
            using (var store = GetDocumentStore())
            {
                new ContentDescriptorByMetadata().Execute(store);
            }
        }
    }
}
