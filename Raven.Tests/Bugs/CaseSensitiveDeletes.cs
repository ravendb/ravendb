using System;
using System.Linq;

using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Bugs
{
	public class CaseSensitiveDeletes : RavenTest
	{
		public class Document
		{
			public string Id { get; set; }
		}

		[Theory]
        [PropertyData("Storages")]
		public void ShouldWork_WithCount(string storageName)
		{
			using (var documentStore = NewDocumentStore(requestedStorage:storageName))
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

		[Theory]
        [PropertyData("Storages")]
		public void ShouldWork_WithAnotherQuery(string storageName)
		{
			using (var documentStore = NewDocumentStore(requestedStorage: storageName))
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
						
						deletes = session.Query<Document>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).ToList();
						Assert.Equal(0, deletes.Count);
					}
				}
			}
		}

	}
}