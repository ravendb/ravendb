// -----------------------------------------------------------------------
//  <copyright file="UsingLoadStartsWithAndDashSepartor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.MailingList
{
    public class UsingLoadStartsWithAndDashSeperator : RavenTest
    {
        [Theory]
        [PropertyData("Storages")]
        public void ShouldWork(string storage)
        {
            using (var store = NewDocumentStore(requestedStorage: storage))
            {


                //store.DocumentDatabase.Documents.Put("delivery-ORT0529.2", null, new RavenJObject(),
                //  new RavenJObject(), null);
                store.DocumentDatabase.Documents.Put("DeliveryReport-1000", null, new RavenJObject(),
                    new RavenJObject(), null);

                store.DocumentDatabase.Documents.Put("delivery-RT0753.2", null, new RavenJObject(),
                  new RavenJObject(), null);

                int nextStart = 0;
                var docs = store.DocumentDatabase.Documents.GetDocumentsWithIdStartingWith("delivery-", null, null,0, 1024, CancellationToken.None, ref nextStart);
                Assert.Equal(1, docs.Length);
            }
        }
    }
}