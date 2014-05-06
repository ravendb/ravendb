using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class Oregon : RavenTest
	{
		[Fact]
		public void CanQueryForOregon()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Where(x => x.LastName == "OR")
						.ToList();
				}
			}
		}

		[Fact]
		public void Fails()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new Dummy { Boolean = false, Object = null });
					s.Store(new Dummy { Boolean = true, Object = null });
					s.Store(new Dummy { Boolean = false, Object = new Dummy() });
					s.Store(new Dummy { Boolean = true, Object = new Dummy() });
					s.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					var objects = s.Query<Dummy>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Boolean || x.Object != null)
						.ToArray();

					Assert.Equal(3, objects.Length); // objects.Length is 2
				}
			}
		}

		public class Dummy
		{
			public bool Boolean { get; set; }
			public Dummy Object { get; set; }
		}
	}
}