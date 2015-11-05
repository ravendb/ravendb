using System;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Raven.Abstractions.Exceptions;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Bugs
{
    public class DeletingDocuments : RavenTest
    {
        [Theory]
        [InlineData("esent")]
        [InlineData("voron")]
        public void ConcurrentDeletes(string storage)
        {
            var store = NewDocumentStore(false, storage);

            var document = new DemoDocument();
            using (var session = store.OpenSession())
            {
                session.Store(document);
                session.SaveChanges();
            }

            var documentLoaded = new CountdownEvent(2);
            var documentDeleted = new CountdownEvent(2);

            var t1 = Task.Run(() => DeleteDocument(store, document.Id, documentLoaded, documentDeleted));
            var t2 = Task.Run(() => DeleteDocument(store, document.Id, documentLoaded, documentDeleted));

            Assert.True(t1.Result | t2.Result, "the document should be deleted");
            Assert.False(t1.Result && t2.Result, "only one operation should complete successfully");
        }

        [Fact]
        public void ConcurrentDeletesWithDtc()
        {
            var store = NewDocumentStore(false, "esent");
            EnsureDtcIsSupported(store);

            var document = new DemoDocument();
            using (var session = store.OpenSession())
            {
                session.Store(document);
                session.SaveChanges();
            }

            var documentLoaded = new CountdownEvent(2);
            var documentDeleted = new CountdownEvent(2);

            var t1 = Task.Run(() =>
            {
                using (var tx = new TransactionScope(TransactionScopeOption.RequiresNew))
                {
                    var result = DeleteDocument(store, document.Id, documentLoaded, documentDeleted);
                    tx.Complete();
                    return result;
                }
            });

            var t2 = Task.Run(() =>
            {
                using (var tx = new TransactionScope(TransactionScopeOption.RequiresNew))
                {
                    var result = DeleteDocument(store, document.Id, documentLoaded, documentDeleted);
                    tx.Complete();
                    return result;
                }
            });

            Assert.True(t1.Result | t2.Result, "the document should be deleted");
            Assert.False(t1.Result && t2.Result, "only one operation should complete successfully");
        }

        private static bool DeleteDocument(IDocumentStore store, Guid documentId, CountdownEvent documentLoaded,
            CountdownEvent documentDeleted)
        {
            try
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;

                    var document = session.Load<DemoDocument>(documentId);

                    documentLoaded.Signal(1);
                    documentLoaded.Wait();

                    session.Delete(document.Id.ToString());

                    documentDeleted.Signal(1);
                    documentDeleted.Wait();

                    session.SaveChanges();
                    return true;
                }
            }
            catch (ConcurrencyException e)
            {
                Console.WriteLine(e);
                return false;
            }
        }

        private class DemoDocument
        {
            public Guid Id { get; set; }
        }
    }
}