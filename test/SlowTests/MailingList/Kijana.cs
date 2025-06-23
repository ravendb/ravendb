using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.MailingList
{
    public class Kijana : RavenTestBase
    {
        public Kijana(ITestOutputHelper output) : base(output)
        {
        }

        private class Scratch
        {
            public string Id { get; set; }
            public long Value { get; set; }
        }

        private class ScratchIndex : AbstractIndexCreationTask<Scratch>
        {
            public ScratchIndex()
            {
                Map = docs =>
                    from doc in docs
                    select new
                    {
                        doc.Value
                    };

            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void CanSetSortValue()
        {
            using (var store = GetDocumentStore())
            {
                new ScratchIndex().Execute(store);
            }
        }
    }
}