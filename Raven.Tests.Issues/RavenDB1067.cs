// -----------------------------------------------------------------------
//  <copyright file="RavenDB1067.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Transactions;

using Raven.Tests.Common;
using Raven.Tests.Common.Util;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB1067 : RavenTest
    {
        public class TestObject {
            public int Value { get; set; }
        }    

        [Fact]
        public void DocumentsNotCommitedIfTransactionIsPromotedToDistributedTx()
        {
            using (var documentStore = NewRemoteDocumentStore(requestedStorage: "esent"))
            {
                if (documentStore.DatabaseCommands.GetStatistics().SupportsDtc == false)
                    return;

                var enlistment = new DummyEnlistmentNotification();
                using (var tx = new TransactionScope())
                {
                    using (var session = documentStore.OpenSession())
                    {
                        session.Store(new TestObject { Value = 1 });
                        session.SaveChanges();
                    }

                    Transaction.Current.EnlistDurable(Guid.NewGuid(), enlistment, EnlistmentOptions.None);
                    tx.Complete();
                }

                using (var session = documentStore.OpenSession())
                {
                    session.Advanced.AllowNonAuthoritativeInformation = false;
                    Assert.NotNull(session.Load<TestObject>(1));
                }
            }
        }
    }
}