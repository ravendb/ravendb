using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Indexes;
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
                               select new {item.Flags};
            }
        }

        [Fact(Skip = "https://github.com/dotnet/roslyn/issues/12045")]
        public void CanWork()
        {
            using(var store = GetDocumentStore())
            {
                store.Conventions.SaveEnumsAsIntegers = true;
                new Index().Execute(store);
            }
        }
    }
}
