using System;
using System.Linq;
using System.Collections.Generic;
using Rhino.Mocks.Constraints;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
   public class RavenDbAnyOfPropertyCollection : LocalClientTest, IDisposable
   {
       IDocumentStore store;

       public RavenDbAnyOfPropertyCollection()
       {
           store = NewDocumentStore();
           using(var session = store.OpenSession())
           {
               session.Store(new Account
               {
                   Transactions =
                       {
                           new Transaction(1),
                           new Transaction(3),
                       }
               });
               session.Store(new Account
               {
                   Transactions =
                       {
                           new Transaction(2),
                           new Transaction(4),
                       }
               });

               session.SaveChanges();
           }
       }

       // works as expected
       [Fact]
       public void ShouldBeAbleToQueryOnTransactionAmount()
       {
           using(var session = store.OpenSession())
           {
               var accounts = session.Query<Account>()
                   .Where(x => x.Transactions.Any(y => y.Amount == 2));
               Assert.Equal(accounts.Count(), 1);
           }
       }

       // This test fails, should return two results but actually reurns zero.
       [Fact]
       public void InequalityOperatorDoesNotWorkOnAny()
       {
           using(var session = store.OpenSession())
           {
               var accounts = session.Query<Account>().Where(x => x.Transactions.Any(y => y.Amount < 3));
               Assert.Equal(accounts.Count(), 2);
           }
       }


       [Fact]
       public void InequalityOperatorDoesNotWorkOnWhereThenAny()
       {
           using(var session = store.OpenSession())
           {
               var accounts = session.Query<Account>().Where(x => x.Transactions.Any(y => y.Amount <= 2));
               Assert.Equal(accounts.Count(), 2);
           }
       }

       public void Dispose()
       {
           if (store != null) store.Dispose();
       }

   }

   public class Account
   {
       public Account()
       {
           Transactions = new List<Transaction>();
       }

       public IList<Transaction> Transactions { get; private set; }
   }

   public class Transaction
   {
       public Transaction(int amount)
       {
           Amount = amount;
       }

       public int Amount { get; private set; }
   }
}