using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class CanUseReduceResultInOutput : RavenTest
	{
		public class CalendarWeek
		{
			public Owner Owner { get; set; }
			public SalesAssignment[] SalesAssignments { get; set; }

			public decimal PendingCount { get; set; }
		}

		public class Owner
		{
			public string OwnerId { get; set; }
		}
		public class SalesAssignment
		{
			public string Status { get; set; }
		}

		public class MyIndex : AbstractIndexCreationTask<CalendarWeek, MyIndex.ReduceResult>
		{
			public class ReduceResult
			{
				public decimal CalendarsCount { get; set; }
				public string OwnerId { get; set; }
				public decimal SoldCount { get; set; }

				public decimal PendingCount { get; set; }
			}


			public MyIndex()
			{
				Map = calendwarWeeks => from calendarWeek in calendwarWeeks
										select new ReduceResult
										{
											OwnerId = calendarWeek.Owner.OwnerId,
											SoldCount = (decimal)calendarWeek.SalesAssignments.Where(x => x.Status == "Sold" || x.Status == "NotSold").Count(),
											CalendarsCount = 1m
										};

				Reduce = records => from record in records
									group record by record.OwnerId
										into g
										let count = g.Sum(x => x.CalendarsCount)
										let sold = g.Sum(x => x.SoldCount)
										select new ReduceResult
										{
											OwnerId = g.Key,
											SoldCount = sold,
											CalendarsCount = count
										};


				Stores.Add(x => x.OwnerId, FieldStorage.Yes);
				Stores.Add(x => x.CalendarsCount, FieldStorage.Yes);
			}
		}

		[Fact]
		public void CanCreateIndex()
		{
			using(var store = NewDocumentStore())
			{
				new MyIndex().Execute(store);
			}
		}
	}
}