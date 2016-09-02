using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Ronne : RavenTestBase
    {
        private class Index : AbstractMultiMapIndexCreationTask
        {
            public Index()
            {
                AddMap<Sermon>(items => from x in items
                                        select new
                                        {
                                            Content = new string[] { x.Description, x.Series, x.Speaker, x.Title }.Union(x.Tags)
                                        });
            }
        }

        [Fact]
        public void CanCreateIndexWithUnion()
        {
            using (var store = GetDocumentStore())
            {
                new Index().Execute(store);
            }
        }

        private class Sermon
        {
#pragma warning disable 649
            public string Description, Series, Speaker, Title;
            public string[] Tags;
#pragma warning restore 649
        }
    }

}
