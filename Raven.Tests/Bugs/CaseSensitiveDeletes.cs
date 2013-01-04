using System;
using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class CaseSensitiveDeletes : RavenTest
	{
		public class Document
		{
			public string Id { get; set; }
		}

		[Fact]
		public void ShouldWork()
		{
			using (var documentStore = NewDocumentStore())
			{
				for (int i = 0; i < 10; i++)
				{
					using (var session = documentStore.OpenSession())
					{
						for (int j = 0; j < 60; j++)
						{
							var doc = new Document
							{
								Id = "CaseSensitiveIndex" + Guid.NewGuid()
							};

							session.Store(doc);
						}

						session.SaveChanges();

						var deletes = session.Query<Document>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).ToList();
						deletes.ForEach(session.Delete);
						session.SaveChanges();

						var count = session.Query<Document>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count();
						Assert.Equal(0, count);
					}
				}
			}
		}
	}
}