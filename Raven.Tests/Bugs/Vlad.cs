using System;
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class Vlad : RavenTest
	{
		public class SampleDocument
		{
			public Guid DocId;
			public string Name;
		}

		public class ProjectedDocument
		{
			public Guid MyDocId;
			public string MyName;
		}

		[Fact]
		public void Test()
		{
			Guid docId = Guid.NewGuid();
			using(var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new SampleDocument
					{
						DocId = docId,
						Name = "Doc1"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var result = session.Query<SampleDocument>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Select(x => new ProjectedDocument
						{
							MyDocId = x.DocId,
							MyName = x.Name
						})
						.First();

					Assert.Equal(result.MyDocId,docId);
					Assert.Equal("Doc1", result.MyName);
				}
			}
		}
	}
}