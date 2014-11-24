using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Database.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.MapRedue
{
	public class Chris : RavenTest
	{
		[Fact]
		public void CanMakeIndexWork()
		{
			using (var store = NewDocumentStore())
			{
				new GroupIndex2().Execute(store);
			}
		}


		[Fact]
		public void IndexWithoutLetShouldWork()
		{
			using(var store = NewDocumentStore())
			{
				new IndexWithoutLet().Execute(store);
			}
		}

		[Fact]
		public void IndexWithLetShouldWork()
		{
			using (var store = NewDocumentStore())
			{
				new IndexWithLet().Execute(store);
			}
		}

		public class IndexWithoutLet : AbstractIndexCreationTask<Chris.CalendarWeek, Record>
		{
			public IndexWithoutLet()
			{
				Map = calendarWeeks => from calendarWeek in calendarWeeks
										select new
										{
											calendarWeek.Owner.OwnerId,
											calendarWeek.PendingCount,
										};

				Reduce = records => from record in records
									group record by record.OwnerId
										into g
										select new
										{
											OwnerId = g.Key,
											PendingCount = g.Sum(x => (decimal)x.PendingCount),
										};

			}
		}

		public class IndexWithLet : AbstractIndexCreationTask<Chris.CalendarWeek, Record>
		{
			public IndexWithLet()
			{
				Map = calendarWeeks => from calendarWeek in calendarWeeks
										select new
										{
											calendarWeek.Owner.OwnerId,
											calendarWeek.PendingCount,
										};

				Reduce = records => from record in records
									group record by record.OwnerId
										into g
										let pendingSum = g.Sum(x => (decimal)x.PendingCount)
										select new
										{
											OwnerId = g.Key,
											PendingCount = pendingSum,
										};


			}
		}

		public class Record
		{
			public decimal CalendarsCount { get; set; }
			public string OwnerId { get; set; }
			public decimal SoldCount { get; set; }

			public decimal PendingCount { get; set; }
		}


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

		public class GroupIndex2 : AbstractIndexCreationTask<CalendarWeek, Record>
		{
			public GroupIndex2()
			{
				Map = calendarWeeks => from calendarWeek in calendarWeeks
										select new
										{
											calendarWeek.Owner.OwnerId,
											SoldCount = (decimal)calendarWeek.SalesAssignments.Where(x => x.Status == "Sold" || x.Status == "NotSold").Count(),
											CalendarsCount = 1m
										};

				Reduce = records => from record in records
									group record by record.OwnerId
										into g
										let count = g.Sum(x => x.CalendarsCount)
										let sold = g.Sum(x => x.SoldCount)
										select new
										{
											OwnerId = g.Key,
											SoldCount = sold,
											CalendarsCount = count
										};


				Stores.Add(x => x.OwnerId, FieldStorage.Yes);
				Stores.Add(x => x.CalendarsCount, FieldStorage.Yes);
			}
		}
	}
}