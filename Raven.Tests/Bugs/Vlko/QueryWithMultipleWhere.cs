using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Vlko
{
	public class QueryWithMultipleWhere : RavenTest
	{
		[Fact]
		public void ShouldGenerateProperPrecedance()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					var query = s.Query<User>()
						.Where(x=>x.Id == "1" || x.Id == "2" || x.Id == "3")
						.Where(x=> x.Age == 19)
						.ToString();

					Assert.Equal("((__document_id:1 OR __document_id:2) OR __document_id:3) AND (Age:19)", query);
				}
			}
		}
	}
}