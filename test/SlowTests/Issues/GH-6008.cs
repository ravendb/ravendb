using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class GH_6008 : RavenTestBase
    {
        private class Item
        {

        }

        [Fact]
        public void CanGetIsLoadedTrueForNewValue()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var item = new Item();
                    s.Store(item, null, id: "items-one");

                    Assert.True(s.Advanced.IsLoaded("items-one"));
                }
            }
        }
    }
}
