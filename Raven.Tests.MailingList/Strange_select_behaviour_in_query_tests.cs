using System.Linq;

using Raven.Tests.Common;
using Raven.Tests.MailingList.PhilJones;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Strange_select_behaviour_in_query_tests : RavenTest
	{
		private class TestCommodityGroup
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public void Select_using_regular_query_should_create_select_list_item()
		{
			using (var store = NewDocumentStore())
			{
				// Arrange
				using (var session = store.OpenSession())
				{
					session.Store(new TestCommodityGroup { Name = "Apples" });
					session.Store(new TestCommodityGroup { Name = "Pears" });
					session.Store(new TestCommodityGroup { Name = "Grapes" });
					session.SaveChanges();

					var query = session.Query<TestCommodityGroup>()
									   .Customize(x=>x.WaitForNonStaleResults())
									   .Select(m => new Projections.SelectListItem { Value = m.Name, Text = m.Name })
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
	}
}