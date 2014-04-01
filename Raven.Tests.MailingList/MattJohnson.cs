using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class MattJohnson : RavenTest
	{
		public class Foo
		{
			public string Id { get; set; }
			public decimal Decimal { get; set; }
			public double Double { get; set; }
			public int Integer { get; set; }
		}

		[Fact]
		public void ShouldSortProperly()
		{
			using(var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Foo { Decimal = 1, Double = 1, Integer = 1 });
					session.Store(new Foo { Decimal = 3, Double = 3, Integer = 3 });
					session.Store(new Foo { Decimal = 10, Double = 10, Integer = 10 });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var query1 = session.Query<Foo>().OrderBy(x => x.Decimal).ToList();
					var query2 = session.Query<Foo>().OrderBy(x => x.Double).ToList();
					var query3 = session.Query<Foo>().OrderBy(x => x.Integer).ToList();

					Assert.True(query1[0].Decimal <= query1[1].Decimal && query1[1].Decimal <= query1[2].Decimal);

					Assert.True(query2[0].Decimal <= query2[1].Decimal && query2[1].Decimal <= query2[2].Decimal);

					Assert.True(query3[0].Decimal <= query3[1].Decimal && query3[1].Decimal <= query3[2].Decimal);
				}
			}
		}
	}
}