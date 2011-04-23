using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class IntegerIds : LocalClientTest
	{
		[Fact]
		public void CanSaveAndLoad()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					s.Store(new WithIntKey
					{
						Name = "abcv"
					});
					s.SaveChanges();
				}

				using(var s = store.OpenSession())
				{
					var load = s.Load<WithIntKey>(1);
					Assert.Equal("abcv", load.Name);
				}
			}
			
		}

		[Fact]
		public void CanSaveAndQuery()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new WithIntKey
					{
						Name = "abcv"
					});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var load = s.Query<WithIntKey>().Where(x => x.Id == 1).First();
					Assert.Equal("abcv", load.Name);
				}
			}

		}

		public class WithIntKey
		{
			public int Id { get; set; }
			public string Name { get; set; }
		}
	}
}