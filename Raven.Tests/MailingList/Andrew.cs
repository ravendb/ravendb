using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Andrew : RavenTest
	{
		[Fact]
		public void CanCompile()
		{
			var technologySummaryIndex = new TechnologySummary_Index {Conventions = new DocumentConvention()};

			var indexDefinition = technologySummaryIndex.CreateIndexDefinition();

			Assert.Equal(
				@"docs.Technologies.Where(technology => !technology.Id.EndsWith(""/published"")).Select(technology => new {
    TechnologyId = technology.Id,
    DrugId = technology.Drug.Id
})",
				indexDefinition.Map);
		}

		public class TechnologySummary_Index : AbstractIndexCreationTask<Technology, TechnologySummary>
		{
			public TechnologySummary_Index()
			{
				Map = (technologies => from technology in technologies
									   where !technology.Id.EndsWith("/published")
									   select new
									   {
										   TechnologyId = technology.Id,
										   DrugId = technology.Drug.Id,
									   });

				Reduce = results => from result in results
									group result by result.TechnologyId
										into g
										let rec = g.LastOrDefault()
										select
											new
											{
												rec.TechnologyId,
												rec.DrugId,
											};
			}
		}

		public class TechnologySummary
		{
			public string TechnologyId;
			public string DrugId;
		}

		public class Technology
		{
			public string Id;
			public Drug Drug;
		}

		public class Drug
		{
			public string Id;
		}
	}
}