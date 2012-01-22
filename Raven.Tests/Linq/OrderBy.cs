using System.Linq;
using Xunit;

namespace Raven.Tests.Linq
{
	public class OrderBy : LocalClientTest
	{
		public class Section
		{
			public string Id { get; set; }
			public int Position { get; set; }
			public string Name { get; set; }

			public Section(int position)
			{
				Position = position;
				Name = string.Format("Position: {0}", position);
			}
		}

		[Fact]
		public void CanDescOrderBy_AProjection()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
						session.Store(new Section(i));
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var lastPosition = session.Query<Section>()
						.Select(x => x.Position)
						.OrderByDescending(x => x)
						.FirstOrDefault();

					Assert.Equal(9, lastPosition);
				}
			}
		}

		[Fact]
		public void CanAscOrderBy_AProjection()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 5; i < 10; i++)
						session.Store(new Section(i));
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					for (int i = 4; i >= 0; i--)
						session.Store(new Section(i));
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var lastPosition = session.Query<Section>()
						.Select(x => x.Position)
						.OrderBy(x => x)
						.FirstOrDefault();

					Assert.Equal(0, lastPosition);
				}
			}
		}
	}
}