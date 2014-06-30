using System;
using System.Transactions;
using Raven.Client.Document;
using Raven.Client.Document.DTC;
using Raven.Tests.Indexes;
using Raven.Tests.Issues;
using Xunit;

namespace Raven.Tryouts
{
    class Program
    {
        private static void Main(string[] args)
        {
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "test",
                TransactionRecoveryStorage = new IsolatedStorageTransactionRecoveryStorage()
            }.Initialize())
            {
                using (var tx = new TransactionScope())
                {
                    using (var documentSession = store.OpenSession())
                    {
                        documentSession.Store(new {Name = "Oren", DateTime.Now}, "users/1");
                        documentSession.SaveChanges();
                    }
                    tx.Complete();
                }
            }
        }
    }
}