using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class EnumAsInts : RavenTestBase
    {
        private enum Flags
        {
            One = 1,
            Two = 2,
            Four = 4
        }
        private class Item
        {
            public Flags Flags { get; set; }
        }

        private class Index : AbstractIndexCreationTask<Item>
        {
            public Index()
            {
                Map = items => from item in items
                               where (item.Flags & Flags.Four) == Flags.Four
                               select new { item.Flags };
            }
        }

        private class Index2 : AbstractIndexCreationTask<Item>
        {
            public Index2()
            {
                Map = items => from item in items
                    where (item.Flags & Flags.Four) == item.Flags
                    select new { item.Flags };
            }
        }

        [Fact]
        public void CanWork()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.SaveEnumsAsIntegers = true;
                }
            }))
            {
                new Index().Execute(store);
                new Index2().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item()
                    {
                        Flags = Flags.Four
                    });

                    session.SaveChanges();

                    var items = session.Query<Item, Index>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    Assert.Equal(1, items.Count);

                    items = session.Query<Item, Index2>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    Assert.Equal(1, items.Count);
                }
            }
        }
    }
}
