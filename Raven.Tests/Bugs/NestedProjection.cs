using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class NestedProjection : RavenTest
	{
		public class SalesDto
		{
			public string Owner { get; set; }
			public string SalesAssignment { get; set; }
		}

		public class CalendarWeek
		{
			public string Owner { get; set; }
			public string[] SalesAssignments { get; set; }
		}

		public class SalesAssignmentsIndex : AbstractIndexCreationTask<CalendarWeek, SalesDto>
		{
			public SalesAssignmentsIndex()
			{
				Map = calendarWeeks => from week in calendarWeeks
									   from assignment in week.SalesAssignments
									   select new { week.Owner, SalesAssignment = assignment };

				Stores.Add(x => x.Owner, FieldStorage.Yes);
				Stores.Add(x => x.SalesAssignment, FieldStorage.Yes);
			}
		}

		[Fact]
		public void CanProject()
		{
			using(var store = NewDocumentStore())
			{
				new SalesAssignmentsIndex().Execute(store);
				using (var session = store.OpenSession())
				{
					session.Store(new CalendarWeek
					{
						Owner = "Oren",
						SalesAssignments = new string[]
						{
							"Write Code",
							"Write Docs"
						}
					});
					session.SaveChanges();
				}
				using(var session = store.OpenSession())
				{
					var results = (from salesDto in session.Query<SalesDto, SalesAssignmentsIndex>()
								   .Customize(x=>x.WaitForNonStaleResults())
					               select salesDto.SalesAssignment).ToList();
					Assert.Equal(2, results.Count);
					Assert.Equal("Write Code", results[0]);
					Assert.Equal("Write Docs", results[1]);
				}
			}
		}
	}

	
}