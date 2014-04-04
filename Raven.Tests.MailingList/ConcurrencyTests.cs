using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class ConcurrencyTests : RavenTestBase
	{
		[Fact]
		public void CanSaveReferencingAndReferencedDocumentsInOneGo()
		{
			using (var store = NewDocumentStore(requestedStorage:"esent")) 
			{
				new Sections().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new SectionData{Id = "sections/1", Referencing = null});
					session.Store(new SectionData { Id = "sections/2", Referencing = "sections/1" });
					session.SaveChanges();
				}

				WaitForIndexing(store);
				using (var session = store.OpenSession())
				{
					session.Advanced.UseOptimisticConcurrency = true;
					var foos = session.Query<SectionData>().ToList();

					foreach (var sectionData in foos)
					{
						sectionData.Count++;
					}

					session.SaveChanges();
				}
			}
		}

		public class SectionData
		{
			public string Id { get; set; }
			public string Referencing { get; set; }
			public int Count { get; set; }
		}

		public class Sections : AbstractIndexCreationTask<SectionData>
		{
			public Sections()
			{
				Map = datas =>
				      from data in datas
				      select new
				      {
					      _ = LoadDocument<SectionData>(data.Referencing)
				      };
			}
		}
	}
}