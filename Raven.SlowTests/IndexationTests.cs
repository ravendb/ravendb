// -----------------------------------------------------------------------
//  <copyright file="IndexationTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.SlowTests
{
    public class IndexationTests : ReplicationBase
    {
        public class Item
        {
            public string Id { get; set; }
            public byte[] Content { get; set; }
            public string Name { get; set; }
        }

        public class Result
        {
            public string Tag { get; set; }
        }

        [Fact]
        public void ShouldWork()
        {
            var one = CreateStore();
            var two = CreateStore();

            var random = new Random();
            var content = new byte[10000];
            random.NextBytes(content);

            string id = null;

            using (var s1 = one.OpenSession())
            {
                for (int i = 0; i < 10000; i++)
                {
                    var item = new Item { Name = "ayende", Content = content };
                    s1.Store(item);

                    id = item.Id;
                }

                s1.SaveChanges();
            }

            // master / master
            TellFirstInstanceToReplicateToSecondInstance();
            Thread.Sleep(2000);

            for (int i = 0; i < 10; i++)
            {
                using (var s2 = two.OpenSession())
                {
                    var item = new Person { Name = "ayende" };
                    s2.Store(item);

                    s2.SaveChanges();

                    Thread.Sleep(500);
                }
            }

            Console.WriteLine("Replication set-up, waiting for replication");
            WaitForReplicationWithDynamicTimeout(two,id,TimeSpan.FromSeconds(15));

            using (var s2 = two.OpenSession())
            {
                var count = s2
                    .Query<Result>("Raven/DocumentsByEntityName")
                    .Customize(x => x.WaitForNonStaleResults())
                    .Count(x => x.Tag == "Items");

                Assert.Equal(10000, count);
            }
        }
    

    private static void WaitForReplicationWithDynamicTimeout(DocumentStore store, string id, TimeSpan timeout)
        {
            var lastId = string.Empty;
            var lastEtag = Etag.Empty;
            var start = DateTime.UtcNow;

            while (DateTime.UtcNow - start <= timeout &&
                   !lastId.Equals(id))
            {
                var docs = store.DatabaseCommands.GetDocuments(lastEtag, 1024);
                var lastDoc = docs.Where(x => !x.Key.StartsWith("Raven/")).OrderBy(x => x.Etag).LastOrDefault();

                if (lastDoc != null)
                {
                    lastEtag = lastDoc.Etag;
                    if (!lastDoc.Key.Equals(lastId))
                        timeout = timeout.Add(TimeSpan.FromSeconds(5));

                    lastId = lastDoc.Key;
                }

                Thread.Sleep(50);
            }
        }
    }
}
