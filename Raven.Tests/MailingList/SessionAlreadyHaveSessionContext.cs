// -----------------------------------------------------------------------
//  <copyright file="SessionAlreadyHaveSessionContext.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Impl.DTC;
using Raven.Storage.Esent;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class SessionAlreadyHaveSessionContext : RavenTest
    {
        [Fact]
        public void Repro()
        {
            using (var store = NewDocumentStore(requestedStorage: "esent"))
            {
                var inFlightTransactionalState = ((EsentInFlightTransactionalState)store.DocumentDatabase.InFlightTransactionalState);
                var transactionalStorage = (TransactionalStorage)store.DocumentDatabase.TransactionalStorage;
                using (var context = inFlightTransactionalState.CreateEsentTransactionContext())
                {
                    using (transactionalStorage.SetTransactionContext(context))
                    {
                        using (context.EnterSessionContext())
                        {

                            transactionalStorage.Batch(accessor =>
                            {
                                transactionalStorage.Batch(x =>
                                {

                                });
                            });

                            transactionalStorage.Batch(accessor =>
                            {

                            });
                        }
                    }
                }
            }
        }
    }
}