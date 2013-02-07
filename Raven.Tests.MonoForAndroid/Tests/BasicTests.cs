using NUnit.Framework;

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