using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3300 : RavenTestBase
	{
		public class Car
		{
			public String Model { get; set; }
			public String Color { get; set; }
			public int Year { get; set; }

		}
		[Fact]
		public void ExposeResultEtagInStatistics()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					var query = session.Query<Car>()
						.Statistics(out stats)
						.Where(x => x.Color == "Blue")
						.ToList();
					var resultEtag = stats.ResultEtag;
					Assert.NotNull(resultEtag);
					Assert.NotEqual(resultEtag, Etag.Empty);
				}
			}
		}
	}
}
