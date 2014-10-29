using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class Random : RavenTest
	{
		[Fact]
		public void CanSortRandomly()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						s.Store(new {Val = i});
					}
					s.SaveChanges();
				}

				using(var s = store.OpenSession())
				{
					var list1 = s.Query<dynamic>()
						.Customize(x=>x.WaitForNonStaleResults().RandomOrdering("seed1"))
						.ToList()
						.Select(x =>(int) x.Val)
						.ToList();

					var list2 = s.Query<dynamic>()
						.Customize(x => x.WaitForNonStaleResults().RandomOrdering("seed2"))
						.ToList()
						.Select(x=>(int)x.Val)
						.ToList();

					Assert.False(list1.SequenceEqual(list2));

				}
			}
		}
	}
}