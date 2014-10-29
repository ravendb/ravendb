using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class indexes_error_CS1977_Cannot_use_a_lambda_expression_from_reduce : RavenTest
	{
		public class Account
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class User
		{
			public string AccountId { get; set; }
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class Design
		{
			public string AccountId { get; set; }
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class ReduceResult
		{
			public string DocumentType { get; set; }
			public string AccountId { get; set; }
			public string AccountName { get; set; }
			public IEnumerable<string> UserName { get; set; }
			public IEnumerable<string> DesignName { get; set; }
		}

		public class ComplexIndex : AbstractMultiMapIndexCreationTask<ReduceResult>
		{
			public ComplexIndex()
			{
				AddMap<Account>(accounts =>
					from account in accounts
					select new
					{
						DocumentType = "Account",
						AccountId = account.Id,
						AccountName = account.Name,
						DesignName = "",
						UserName = "",
					});
				AddMap<Design>(designs =>
					from design in designs
					select new
					{
						DocumentType = "Design",
						AccountId = design.AccountId,
						AccountName = "",
						DesignName = design.Name,
						UserName = "",
					});
				AddMap<User>(users =>
					from user in users
					select new
					{
						DocumentType = "User",
						AccountId = user.AccountId,
						AccountName = "",
						DesignName = "",
						UserName = user.Name,
					});

				Reduce = results =>
					from result in results
					group result by result.AccountId into accountGroup
					from account in accountGroup
					where account.DocumentType == "Account"
					select new
					{
						DocumentType = "Account",
						AccountId = account.AccountId,
						AccountName = account.AccountName,
						UserName = accountGroup.Where(x => x.DocumentType == "User").SelectMany(x => x.UserName),
						DesignName = accountGroup.Where(x => x.DocumentType == "Design").SelectMany(x => x.DesignName)
					};
			}
		}

		public class SelectIndex : AbstractMultiMapIndexCreationTask<ReduceResult>
		{
			public SelectIndex()
			{
				{
					AddMap<Account>(accounts =>
						from account in accounts
						select new
						{
							DocumentType = "Account",
							AccountId = account.Id,
							AccountName = account.Name,
							DesignName = "",
							UserName = "",
						});
					AddMap<Design>(designs =>
						from design in designs
						select new
						{
							DocumentType = "Design",
							AccountId = design.AccountId,
							AccountName = "",
							DesignName = design.Name,
							UserName = "",
						});
					AddMap<User>(users =>
						from user in users
						select new
						{
							DocumentType = "User",
							AccountId = user.AccountId,
							AccountName = "",
							DesignName = "",
							UserName = user.Name,
						});

					Reduce = results =>
						from result in results
						group result by result.AccountId into accountGroup
						from account in accountGroup
						where account.DocumentType == "Account"
						select new
						{
							DocumentType = "Account",
							AccountId = account.AccountId,
							AccountName = account.AccountName,
							UserName = accountGroup.Where(x => x.DocumentType == "User").Select(x => x.UserName),
							DesignName = accountGroup.Where(x => x.DocumentType == "Design").Select(x => x.DesignName)
						};
				}
			}
		}


		[Fact]
		public void can_create_index()
		{
			using (var store = NewDocumentStore())
			{
				new ComplexIndex().Execute(store);
			}
		}


		[Fact]
		public void can_create_index_where_reduce_uses_select()
		{
			using (var store = NewDocumentStore())
			{
				new SelectIndex().Execute(store);
			}
		}
	}
}