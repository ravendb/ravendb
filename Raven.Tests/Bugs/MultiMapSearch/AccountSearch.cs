using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Client.Linq.Indexing;

namespace Raven.Tests.Bugs.MultiMapSearch
{
	public class AccountSearch : AbstractMultiMapIndexCreationTask<AccountSearch.ReduceResult>
	{
		public class ReduceResult
		{
			public string Id { get; set; }
			public int PortalId { get; set; }
			public string Query { get; set; }
			public string QueryBoosted { get; set; }

			public string SortField { get; set; }
		}

		public AccountSearch()
		{
			AddMap<Organization>(organizations => from org in organizations
												select new
												{
													Id = org.Id,
													PortalId = org.PortalId,
													SortField = org.Name,
													Query = new object[]
													{
														org.Name,
													},
													QueryBoosted = new object[]
													{
														org.Name
													}.Boost(3),
												});
			AddMap<Person>(customers => from c in customers
										select new
										{
											Id = c.Id,
											PortalId = c.PortalId,
											SortField = c.LastName,
											Query = new object[]
											{
												string.Format("{0} {1} {2}", c.FirstName, c.MiddleName, c.LastName),
											},
											QueryBoosted = new object[]
											{
												string.Format("{0} {1} {2}", c.FirstName, c.MiddleName, c.LastName)
											}.Boost(3),
										});

			Index(x => x.Query, FieldIndexing.Analyzed);
			Index(x => x.QueryBoosted, FieldIndexing.Analyzed);
		}
	}

	public class Account
	{
		public string Id { get; set; }
		public string PortalId { get; set; }
	}

	public class Organization : Account
	{
		public string Name { get; set; }
	}

	public class Person : Account
	{
		public string FirstName { get; set; }

		public string MiddleName { get; set; }

		public string LastName { get; set; }
	}
}