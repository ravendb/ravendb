// -----------------------------------------------------------------------
//  <copyright file="OrdersByEmployeeAndCompanyReduce.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;

namespace Raven.Tests.Migration.Indexes
{
	public class OrdersByEmployeeAndCompanyReduce : AbstractIndexCreationTask
	{
		public class Result
		{
			public string OrderId { get; set; }

			public string LastName { get; set; }

			public string FirstName { get; set; }

			public string Company { get; set; }

			public int Count { get; set; }
		}

		public override string IndexName
		{
			get
			{
				return "Orders/ByEmployeeAndCompany/Reduce";
			}
		}
		public override IndexDefinition CreateIndexDefinition()
		{
			return new IndexDefinition
			{
				Map = @"from order in docs.Orders
let employee = LoadDocument(order.Employee)
let company = LoadDocument(order.Company)
select new {
    OrderId = order.Id,
    LastName = employee.LastName,
    FirstName = employee.FirstName,
    Company = company.Name,
    Count = 1
}",
				Reduce = @" from result in results
 group result by result.OrderId into g
 select new {
    OrderId = g.Key,
    Company = g.Select(x => x.Company).FirstOrDefault(),
    LastName = g.Select(x => x.LastName).FirstOrDefault(),
    FirstName = g.Select(x => x.FirstName).FirstOrDefault(),
    Count = g.Sum(x => x.Count)
 }"
			};
		}
	}
}