using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class MultipleRangeQueries : RavenTest
	{
		[Fact]
		public void CanQueryOnSameFieldMultipleTimesUsingRanges()
		{
			using(GetNewServer())
			using(var store = new DocumentStore{Url = "http://localhost:8079"}.Initialize())
			{
				using(var s = store.OpenSession())
				{
					s.Query<User>()
						.Where(x => x.Age < 10 && x.Age >= 18)
						.OrderBy(x=>x.Age)
						.ToList();
				}
			}
		}
	}
}