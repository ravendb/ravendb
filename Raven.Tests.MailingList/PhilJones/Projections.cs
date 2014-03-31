using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList.PhilJones
{
	public class Projections : RavenTest
	{
		[Fact]
		public void WorkWithRealTypes()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Query<Offer>()
						.Where(x => x.TripId == "trips/1234")
						.OrderBy(x => x.Name)
						.Select(x => new SelectListItem
						{
							Text = x.Name,
							Value = x.Id
						})
						.ToList();
				}
			}
		}

		public class SelectListItem
		{
			public string Text { get; set; }
			public string Value { get; set; }
		}


		public class Offer
		{
			public string Id { get; set; }
			public string TripId { get; set; }
			public string Name { get; set; }
		}
	}

}