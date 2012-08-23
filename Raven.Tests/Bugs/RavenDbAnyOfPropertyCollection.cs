//-----------------------------------------------------------------------
// <copyright file="AnyQueries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class RavenDbAnyOfPropertyCollection : RavenTest
	{
		private readonly IDocumentStore store;
		private readonly DateTime now = new DateTime(2010, 10, 31);

		public RavenDbAnyOfPropertyCollection()
		{
			store = NewDocumentStore();
			using (var session = store.OpenSession())
			{
				session.Store(new Account
				{
					Transactions =
					  {
						  new Transaction(1, now.AddDays(-2)),
						  new Transaction(3, now.AddDays(-1)),
					  }
				});
				session.Store(new Account
				{
					Transactions =
					  {
						  new Transaction(2, now.AddDays(1)),
						  new Transaction(4, now.AddDays(2)),
					  }
				});
				session.SaveChanges();
			}
		}

		public override void Dispose()
		{
			if (store != null) store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void ShouldBeAbleToQueryOnTransactionAmount()
		{
			using (var session = store.OpenSession())
			{
				var accounts = session.Query<Account>()
					.Where(x => x.Transactions.Any(y => y.Amount == 2));
				Assert.Equal(accounts.Count(), 1);
			}
		}

		[Fact]
		public void InequalityOperatorDoesNotWorkOnAny()
		{
			using (var session = store.OpenSession())
			{
				var accounts = session.Query<Account>().Where(x => x.Transactions.Any(y => y.Amount < 3));
				Assert.Equal(accounts.Count(), 2);
			}
		}


		[Fact]
		public void InequalityOperatorDoesNotWorkOnWhereThenAny()
		{
			using (var session = store.OpenSession())
			{
				var accounts = session.Query<Account>().Where(x => x.Transactions.Any(y => y.Amount <= 2));
				Assert.Equal(accounts.Count(), 2);
			}
		}

		[Fact]
		public void CanSelectADateRange()
		{
			using (var session = store.OpenSession())
			{
				var accounts = session.Query<Account>().Where(x => x.Transactions.Any(y => y.Date < now));
				var array = accounts.ToArray();
				Assert.Equal(1, array.Count());
			}
		}
	}
}