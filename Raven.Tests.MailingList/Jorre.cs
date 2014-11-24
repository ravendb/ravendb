using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class Jorre : RavenTest
	{
		[Fact]
		public void CanQueryOnNegativeDecimal()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Query<Boat>()
						.Where(x => x.Weight == -1)
						.ToList();
				}
			}
		}

		public class Boat
		{
			public decimal Weight { get; set; }
		}
	}
}