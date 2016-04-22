
// -----------------------------------------------------------------------
//  <copyright file="DataInconsistencyRepro.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using SlowTests.Utils;
using Voron;
using Voron.Data.BTrees;
using Xunit;

namespace SlowTests.Voron.Bugs
{
    public class DataInconsistencyRepro : StorageTest
    {
        [Theory]
        [InlineDataWithRandomSeed(5000, 10000)]
        [InlineDataWithRandomSeed(1000, 5000)]
        [InlineDataWithRandomSeed(1000, 50000)]
        [InlineData(1000, 1500, 1396255086)]
        [InlineData(1000, 5000, 1430320600)]
        public void FaultyOverflowPagesHandling_CannotModifyReadOnlyPages(int initialNumberOfDocs, int numberOfModifications, int seed)
        {
            const string documents = "documents";
            const string keyByEtag = "documents_key_by_etag";
            const string metadata = "documents_metadata";

            var inMemoryKeysByEtag = new Dictionary<Guid, string>();
            var inMemoryKeys = new HashSet<string>();
            var r = new Random(seed);
            var uuidGenerator = new UuidGenerator();

            using (var tx = Env.WriteTransaction())
            {
                var docsTree = tx.CreateTree(documents);
                var metadataTree = tx.CreateTree(metadata);
                var indexTree = tx.CreateTree(keyByEtag);

                for (int i = 0; i < initialNumberOfDocs; i++)
                {
                    var etag = uuidGenerator.CreateSequentialUuid();
                    var docKey = get_id(etag, r);

                    put_doc(r, etag, inMemoryKeysByEtag, inMemoryKeys, docKey, docsTree, metadataTree, indexTree);
                }

                tx.Commit();
            }

            for (int i = 0; i < numberOfModifications; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var docsTree = tx.ReadTree(documents);
                    var metadataTree = tx.ReadTree(metadata);
                    var indexTree = tx.ReadTree(keyByEtag);

                    if (r.Next(3) == 0)
                    {
                        // insert new
                        var etag = uuidGenerator.CreateSequentialUuid();
                        var docKey = get_id(etag, r);

                        put_doc(r, etag, inMemoryKeysByEtag, inMemoryKeys, docKey, docsTree, metadataTree, indexTree);
                    }
                    else
                    {
                        // update existing
                        var docCount = inMemoryKeysByEtag.Values.Count;

                        var docKeyToUpdate = inMemoryKeysByEtag.Values.Skip(r.Next(0, docCount - 1)).First();
                        var etag = uuidGenerator.CreateSequentialUuid();

                        put_doc(r, etag, inMemoryKeysByEtag, inMemoryKeys, docKeyToUpdate, docsTree, metadataTree, indexTree);
                    }

                    tx.Commit();
                }
            }

            using (var tx = Env.ReadTransaction())
            {
                var docsTree = tx.ReadTree(documents);
                var metadataTree = tx.ReadTree(metadata);
                var keyByEtagTree = tx.ReadTree(keyByEtag);

                var count = 0;
                using (var iterator = keyByEtagTree.Iterate())
                {
                    iterator.Seek(Slice.BeforeAllKeys);
                    do
                    {
                        var etag = Guid.Parse(iterator.CurrentKey.ToString());

                        var reader = iterator.CreateReaderForCurrent();

                        var key = reader.ReadString(reader.Length);

                        var inMemoryKey = inMemoryKeysByEtag[etag];

                        Assert.Equal(inMemoryKey, key);

                        var docReadResult = docsTree.Read(key);

                        Assert.NotNull(docReadResult);

                        var metadataReader = metadataTree.Read(key).Reader;

                        Assert.NotNull(metadataReader);

                        var etagFromMetadata = new byte[16];
                        metadataReader.Read(etagFromMetadata, 0, 16);

                        var readEtag = new Guid(etagFromMetadata);
                        if (etag != readEtag)
                        {
                            string existingDocKey;
                            if (inMemoryKeysByEtag.TryGetValue(readEtag, out existingDocKey))
                            {
                                Console.WriteLine("Etag " + readEtag + " belongs to " + existingDocKey + " document");
                            }
                            else
                            {
                                Console.WriteLine("There is no document with etag " + readEtag);
                            }
                        }

                        Assert.Equal(etag, readEtag);

                        count++;
                    }
                    while (iterator.MoveNext());
                }

                Assert.Equal(inMemoryKeysByEtag.Count, count);
            }
        }

        private void put_doc(Random r, Guid etag, Dictionary<Guid, string> inMemoryKeysByEtag, HashSet<string> inMemoryKeys, string docKey, Tree docsTree, Tree metadataTree, Tree indexTree)
        {
            var docMetadata = new byte[r.Next(100, 7000)];

            Array.Copy(etag.ToByteArray(), docMetadata, 16);

            if (inMemoryKeys.Add(docKey) == false)
            {
                // update
                var existingEtag = inMemoryKeysByEtag.First(x => x.Value == docKey).Key;

                indexTree.Delete(existingEtag.ToString());
                inMemoryKeysByEtag.Remove(existingEtag);
            }

            docsTree.Add(docKey, get_doc_data(r));
            metadataTree.Add(docKey, docMetadata);
            indexTree.Add(etag.ToString(), docKey);

            inMemoryKeysByEtag[etag] = docKey;
        }

        private string get_id(Guid etag, Random r)
        {
            return string.Format("trackings/{0}/{1}/2016-02", etag, get_ip_address(r));
        }

        private string get_doc_data(Random r)
        {
            return @"{
  ""LicenseId"": ""11111111-2222-3333-4444-555555555555"",
  ""Opened"": ""2016-02-03T13:15:18.2820515Z"",
  ""ProductInfo"": {
    ""ProductType"": ""RavenDb"",
    ""FullProductName"": ""RavenDB ISV Server (Bundles: fips) - Yearly Subscription"",
    ""CustomerId"": ""customers/1234"",
    ""LicenseType"": ""Subscription"",
    ""Quantity"": 5,
    ""IsOem"": true
  },
  ""CountByDay"": {
                ""2016-02-03T00:00:00.0000000"": 1
  },
  ""Error"": null,
  ""Ip"": """ + get_ip_address(r) + @"""
}";
        }

        private string get_ip_address(Random r)
        {
            return string.Format("{0}.{1}.{2}.{3}", r.Next(0, 255), r.Next(0, 255), r.Next(0, 255), r.Next(0, 255));
        }

        public class UuidGenerator
        {
            private readonly long currentEtagBase = 0;
            private long sequentialUuidCounter;

            public Guid CreateSequentialUuid()
            {
                var ticksAsBytes = BitConverter.GetBytes(currentEtagBase);
                Array.Reverse(ticksAsBytes);
                var increment = Interlocked.Increment(ref sequentialUuidCounter);
                var currentAsBytes = BitConverter.GetBytes(increment);
                Array.Reverse(currentAsBytes);
                var bytes = new byte[16];
                Array.Copy(ticksAsBytes, 0, bytes, 0, ticksAsBytes.Length);
                Array.Copy(currentAsBytes, 0, bytes, 8, currentAsBytes.Length);
                return new Guid(bytes);
            }
        }
    }
}