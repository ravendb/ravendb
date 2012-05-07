using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class Oregon : RavenTest
	{
		[Fact]
		public void CanQueryForOregon()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Query<User>()
						.Where(x => x.LastName == "OR")
						.ToList();
				}
			}
		}
	}
}