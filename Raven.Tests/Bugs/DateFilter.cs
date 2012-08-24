using System;
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class DateFilter : RavenTest
	{
		[Fact]
		public void WhenDefiningIndexWithSystemType_IndexShouldGetDefined()
		{
			using (var store = NewDocumentStore())
				new Orders_BySentDate().Execute(store);
		}

		public class Orders_BySentDate : AbstractIndexCreationTask<Order>
		{
			public Orders_BySentDate()
			{
				Map = orders => from o in orders
								where o.SentDate >= new DateTime(2011, 5, 23, 0, 0, 0, DateTimeKind.Utc)
								select new { o.Id };
			}
		}

		public class Order
		{
			public string Id { get; set; }
			public DateTime SentDate { get; set; }
		}
	}
}