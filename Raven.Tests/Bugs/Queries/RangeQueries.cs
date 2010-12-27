using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs.Queries
{
	public class RangeQueries : LocalClientTest
	{
		[Fact]
		public void CanQueryOnRangeEqualsInt()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new WithInteger { Sequence = 1 });
					session.Store(new WithInteger { Sequence = 2 });

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var withInt = session.Query<WithInteger>().Where(x => x.Sequence >= 1).ToArray();
					Assert.Equal(2, withInt.Length);
				}
			}
		}

		[Fact]
		public void CanQueryOnRangeEqualsLong()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new WithLong { Sequence = 1 });
					session.Store(new WithLong { Sequence = 2 });

					session.SaveChanges();
				}


				using (var session = store.OpenSession())
				{
					var withLong = session.Query<WithLong>().Where(x => x.Sequence >= 1).ToArray();
					Assert.Equal(2, withLong.Length);
				}
			}
		}

		[Fact]
		public void CanQueryOnRangeInt()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new WithInteger { Sequence = 1 });
					session.Store(new WithInteger { Sequence = 2 });

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var withInt = session.Query<WithInteger>().Where(x => x.Sequence > 0).ToArray();
					Assert.Equal(2, withInt.Length);
				}
			}
		}

		[Fact]
		public void CanQueryOnRangeLong()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new WithLong { Sequence = 1 });
					session.Store(new WithLong { Sequence = 2 });

					session.SaveChanges();
				}


				using (var session = store.OpenSession())
				{
					var withLong = session.Query<WithLong>().Where(x => x.Sequence > 0).ToArray();
					Assert.Equal(2, withLong.Length);
				}
			}
		}

		public class WithInteger
		{
			public int Sequence { get; set; }
		}
		public class WithLong
		{
			public long Sequence { get; set; }
		}
	}
}