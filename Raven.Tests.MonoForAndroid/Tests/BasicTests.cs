using NUnit.Framework;
using Raven.Client.Linq;
using System.Linq;

namespace Raven.Tests.MonoForAndroid
{
	public class BasicTests : MonoForAndroidTestBase
	{
		[Test]
		public void CanLoadFromServer()
		{
			using (var store = CreateDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Info(), "infos/1234");
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var info = session.Load<Info>(1234);
					Assert.NotNull(info);
				}
			}
		}

		[Test]
		public void CanQuery()
		{
			using (var store = CreateDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Info{Data = "First"});
					session.Store(new Info{Data = "Other"});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.IsNotEmpty(session.Query<Info>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.Where(x => x.Data == "First").ToList());
				}
			}
		}

		[Test]
		public void CanWriteToServer()
		{
			using (var store = CreateDocumentStore())
			using (var session = store.OpenSession())
			{
				session.Store(new Info());
				session.SaveChanges();
			}
		}

	}
}