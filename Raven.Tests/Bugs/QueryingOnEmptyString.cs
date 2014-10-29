using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class QueryingOnEmptyString : RavenTest
	{

		[Fact]
		public void ShouldNotSelectAllDocs()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "Ayende" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.Empty(session.Query<User>()
										.Where(x => x.Name == string.Empty)
										.ToList());
				}
			}
		}


		[Fact]
		public void CanFindByemptyStringMatch()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.NotEmpty(session.Query<User>()
										.Where(x => x.Name == string.Empty)
										.ToList());
				}
			}
		}
	}
}