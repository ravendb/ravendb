using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class Strange_select_behaviour_in_query_tests : RavenTestBase
    {
        private class TestCommodityGroup
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void Select_using_regular_query_should_create_select_list_item()
        {
            using (var store = GetDocumentStore())
            {
                // Arrange
                using (var session = store.OpenSession())
                {
                    session.Store(new TestCommodityGroup { Name = "Apples" });
                    session.Store(new TestCommodityGroup { Name = "Pears" });
                    session.Store(new TestCommodityGroup { Name = "Grapes" });
                    session.SaveChanges();

                    var query = session.Query<TestCommodityGroup>()
                                       .Customize(x => x.WaitForNonStaleResults())
                                       .Select(m => new SelectListItem { Value = m.Name, Text = m.Name })
                                       .ToArray();

                    Assert.NotEmpty(query);
                    foreach (var selectListItem in query)
                    {
                        Assert.NotNull(selectListItem.Value);
                        Assert.NotNull(selectListItem.Text);
                    }
                }
            }
        }

        private class SelectListItem
        {
            public string Text { get; set; }
            public string Value { get; set; }
        }
    }
}
