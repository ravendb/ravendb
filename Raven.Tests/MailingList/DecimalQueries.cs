using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class DecimalQueries : RavenTest
	{
		public class Money
		{
			public decimal Amount { get; set; }
		}

		[Fact]
		public void CanQuery()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Money
					{
						Amount = 10.00m
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var moneys = session.Query<Money>()
						.Where(x=>x.Amount == 10.000m)
						.ToArray();

					Assert.NotEmpty(moneys);
				}
			}
		}
	}
}