using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class SpecialChars : RavenTest
	{
		[Fact]
		public void ShouldWork()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Where(x => x.LastName == "abc&edf")
						.ToList();
				}
			}
		}

		[Fact]
		public void ShouldWork_Remote()
		{
			using(GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Where(x => x.LastName == "abc&edf")
						.ToList();
				}
			}
		}
	}
}