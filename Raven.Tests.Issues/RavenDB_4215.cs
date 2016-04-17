// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4215.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_4215 : RavenTest
    {
        [Theory]
        [PropertyData("Storages")]
        public void touch_index_etag_if_index_doesnt_exist(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.Indexing.AddIndex(101, true));

                storage.Batch(accessor =>
                {
                    var stat1 = accessor.Indexing.GetIndexStats(101);
                    Assert.Equal(101, stat1.Id);
                    Assert.Equal(0, stat1.TouchCount);
                });

                storage.Batch(accessor => accessor.Indexing.TouchIndexEtag(101));

                storage.Batch(accessor =>
                {
                    var stat1 = accessor.Indexing.GetIndexStats(101);
                    Assert.Equal(101, stat1.Id);
                    Assert.Equal(1, stat1.TouchCount);
                });

                storage.Batch(accessor => accessor.Indexing.PrepareIndexForDeletion(101));

                storage.Batch(accessor => accessor.Indexing.TouchIndexEtag(101));
            }
        }
    }
}
