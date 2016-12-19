using System.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Querying
{
    public class Item
    {
        public string Val;
    }

    public class ParserQueryWithPlusOperator : RavenTest
    {
        [Fact]
        public void QueryWithPlusOperator()
        {
            using (var store = NewDocumentStore())
            {
                var val = "+a+b";
                var onlyPlusVal = new string('+', 2408);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Val = val });
                    session.Store(new Item { Val = onlyPlusVal });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Query<Item>()
                        .Where(x => x.Val == val)
                        .ToList());

                    Assert.NotEmpty(session.Query<Item>()
                        .Where(x => x.Val == onlyPlusVal)
                        .ToList());

                    val = "+";
                    onlyPlusVal = "+a";

                    Assert.Empty(session.Query<Item>()
                        .Where(x => x.Val == val)
                        .ToList());

                    Assert.Empty(session.Query<Item>()
                        .Where(x => x.Val == val)
                        .ToList());
                }
            }
        }
    }
}