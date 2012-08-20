// -----------------------------------------------------------------------
//  <copyright file="CanUpdateInsideDtcWithOptimisticConcurrency.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Transactions;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class CanUpdateInsideDtcWithOptimisticConcurrency : RavenTest
	{
		[Fact]
		public void ShouldWork()
		{
			using(var store = NewDocumentStore())
			{
				using(var tx = new TransactionScope())
				using (var s = store.OpenSession())
				{
					s.Store(new User { FirstName = "a" }, "Users/test");
					s.SaveChanges();
					tx.Complete();
				}

				using (var tx = new TransactionScope())
				using (var s = store.OpenSession())
				{
					s.Advanced.AllowNonAuthoritativeInformation = false;
					s.Load<User>("Users/test").FirstName = "b";
					s.SaveChanges();
					tx.Complete();
				}

				using (var tx = new TransactionScope())
				using (var s = store.OpenSession())
				{
					s.Advanced.AllowNonAuthoritativeInformation = false;
					Assert.Equal("b", s.Load<User>("Users/test").FirstName);
					tx.Complete();
				}
			}
		}
	}
}