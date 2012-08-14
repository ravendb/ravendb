using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Spatial;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class Events_ByDate_Count : AbstractIndexCreationTask<Event>
	{
		public Events_ByDate_Count()
		{
			Map = events => from @event in events select new { @event.Date.Year, @event.Date.Month, Count = 1 };
		}
	}

	public class ReservedWords : LocalClientTest
	{
		[Fact]
		public void WillOutputCorrectly()
		{
			var indexDefinition = new Events_ByDate_Count
			{
				Conventions = new DocumentConvention()
			}.CreateIndexDefinition();

			Assert.Equal(@"docs.Events.Select(@event => new {
    Year = @event.Date.Year,
    Month = @event.Date.Month,
    Count = 1
})", indexDefinition.Map);
		}

		[Fact]
		public void can_parse_escaped_reserved_words_correctly()
		{
			using (var store = NewDocumentStore())
			{
				new Events_ByDate_Count().Execute(store);
			}
		}
	}
}
