using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB934 : RavenTest
	{
		public class User
		{
		}

		[Fact]
		public void LowLevelExportsByDoc()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 1500; i++)
					{
						session.Store(new User());
					}
					session.SaveChanges();
				}

				int count = 0;
				using(var streamDocs = store.DatabaseCommands.StreamDocs())
				while (streamDocs.MoveNext())
				{
					count++;
				}
				Assert.Equal(1501, count); // also include the hi lo doc
			}
		}

		[Fact]
		public void LowLevelExportsByDocPrefixRemote()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 1500; i++)
					{
						session.Store(new User());
					}
					session.SaveChanges();
				}

				int count = 0;
				using (var streamDocs = store.DatabaseCommands.StreamDocs(startsWith: "users/"))
				{
					while (streamDocs.MoveNext())
					{
						count++;
					}
				}
				Assert.Equal(1500, count);
			}
		}

		[Fact]
		public void HighLevelExportsByDocPrefixRemote()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 1500; i++)
					{
						session.Store(new User());
					}
					session.SaveChanges();
				}

				int count = 0;
				using (var session = store.OpenSession())
				{
					using (var reader = session.Advanced.Stream<User>(startsWith: "users/"))
					{
						while (reader.MoveNext())
						{
							count++;
							Assert.IsType<User>(reader.Current.Document);
						}
					}
				}
				Assert.Equal(1500, count);
			}
		}
	}
}