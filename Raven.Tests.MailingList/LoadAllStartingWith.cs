using System.Linq;
using Raven.Client;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class LoadAllStartingWith : RavenTestBase
	{

		public class Abc
		{
			public string Id { get; set; }
		}

		public class Xyz
		{
			public string Id { get; set; }
		}

		[Fact]
		public void LoadAllStartingWithShouldNotLoadDeletedDocs()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var doc1 = new Abc()
				{
					Id = "abc/1",
				};
				var doc2 = new Xyz()
				{
					Id = "xyz/1"
				};


				using (var session = store.OpenSession())
				{
					session.Store(doc1);
					session.Store(doc2);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					// commenting out this call passes the test
					var testClasses = session.Advanced.Lazily.LoadStartingWith<Abc>("abc/");
					var test2Classes = session.Query<Xyz>().Customize(x => x.WaitForNonStaleResults())
					                          .Lazily().Value.ToList();

					Assert.Equal(1, testClasses.Value.Count());
					Assert.Equal(1, test2Classes.Count());
				}
			}
		}
	}
}
