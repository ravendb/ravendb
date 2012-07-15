using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			using(var docStore = new DocumentStore
			{
				Url = "http://localhost:8080",
				DefaultDatabase = "RavenHQ.Biling"
			}.Initialize())
			{
				new DatabasePlanSubscriptionSearch().Execute(docStore);
				using(var session = docStore.OpenSession())
				{
					int size;
					foreach (var item in session.ReportFor(new DateTime(2012, 06, 01), 0, 50, out size))
					{
						Console.WriteLine(item.MonthEnd);
					}
				}
			}
		}
	}

	public class ReportResult
	{
		public Guid AccountId { get; set; }
		public Guid DatabaseId { get; set; }
		public Guid PlanId { get; set; }
		public DateTime MonthStart { get; set; }
		public DateTime MonthEnd { get; set; }
	}

	public static class DatabasePlanSubscriptionExtensions
	{
		public static IEnumerable<ReportResult> ReportFor(
			this IDocumentSession session, DateTime month, int start, int pageSize, out int totalSize)
		{
			month = new DateTime(month.Year, month.Month, 1);
			var end = month.AddMonths(1);

			var monthStr = month.ToString("yyyy-MM");
			var query =
				session.Advanced.LuceneQuery<DatabasePlanSubscriptionSearch.Result, DatabasePlanSubscriptionSearch>()
					.WhereLessThanOrEqual(x => x.StartMonth, monthStr)
					.AndAlso()
					.WhereGreaterThanOrEqual(x => x.EndMonth, monthStr)
					.SelectFields<DatabasePlanSubscriptionSearch.Result>()
					.Take(pageSize)
					.Skip(start);
			var results = query.ToList();

			totalSize = query.QueryResult.TotalResults;

			return from result in results
			       let monthStart = (month > result.StartDate) ? month : result.StartDate
			       let monthEnd = (end < result.EndDate) ? end : result.EndDate
			       select new ReportResult
			       {
			       	AccountId = result.AccountId,
			       	DatabaseId = result.DatabaseId,
			       	PlanId = result.PlanId,
			       	MonthStart = monthStart,
			       	MonthEnd = monthEnd
			       };
		}
	}

	public class DatabasePlanSubscriptionSearch : AbstractIndexCreationTask<DatabasePlanSubscription, DatabasePlanSubscriptionSearch.Result>
	{
		public class Result
		{
			public Guid AccountId { get; set; }
			public Guid DatabaseId { get; set; }
			public Guid PlanId { get; set; }
			public DateTime StartDate { get; set; }
			public DateTime EndDate { get; set; }

			public string StartMonth { get; set; }
			public string EndMonth { get; set; }
		}

		public DatabasePlanSubscriptionSearch()
		{
			Map = planSubscriptions =>
			      from databasePlanSubscription in planSubscriptions
			      from sub in databasePlanSubscription.Subscriptions
				  where "0001-01-01T00:00:00".Equals(sub.StartDate) == false
				  let endDate = (sub.EndDate ?? new DateTimeOffset(new DateTime(2020, 1, 1), TimeSpan.Zero))
			      select new
			      {
			      	databasePlanSubscription.AccountId,
			      	databasePlanSubscription.DatabaseId,
			      	sub.PlanId,

					StartMonth = sub.StartDate.ToString("yyyy-MM"),
					EndMonth = endDate.ToString("yyyy-MM"),
					
					sub.StartDate,
			      	EndDate = endDate,
			      };

			StoreAllFields(FieldStorage.Yes);
		}
	}

	public class DatabasePlanSubscription
	{
		public Guid AccountId { get; set; }
		public Guid DatabaseId { get; set; }
		public Subscription[] Subscriptions { get; set; }

		public class Subscription
		{
			public Guid PlanId { get; set; }
			public DateTime StartDate { get; set; }
			public DateTime? EndDate { get; set; }
		}
	}
}