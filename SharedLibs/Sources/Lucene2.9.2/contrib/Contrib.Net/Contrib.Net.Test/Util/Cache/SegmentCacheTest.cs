/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util.Cache;

namespace Lucene.Net.Test.Util.Cache
{
    /// <summary>
    /// Summary description for TestCache
    /// </summary>
    [TestFixture]
    public class CacheTest
    {
        public CacheTest()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        private RAMDirectory directory;

        private IndexReader reader;

        #region setup/teardown methods

        [SetUp]
        public void MyTestInitialize()
        {
            // create the initial index
            this.directory = new RAMDirectory();
            this.CreateInitialIndex(this.directory);
            this.reader = IndexReader.Open(this.directory, true);
        }

        [TearDown]
        public void MyTestCleanup()
        {
            if (this.reader != null)
            {
                this.reader.Close();
                this.reader = null;
            }
            if (this.directory != null)
            {
                this.directory.Close();
                this.directory = null;
            }

            GC.Collect();
        }

        #endregion

        [Test]
        public void CreateAddRetreive_SingleReader()
        {
            // warm the cache
            Terms t = MockCache.Instance.Get(this.reader, "Cached");
            this.AssertCache(t, 1, "Cached", 10);

            // get the item from cache
            t = MockCache.Instance.Get(this.reader, "Cached");
            this.AssertCache(t, 1, "Cached", 10);
        }

        [Test]
        public void CreateAddRetreive_TwoReaders()
        {
            RAMDirectory rd = new RAMDirectory();
            this.CreateInitialIndex(rd);
            IndexReader r2 = IndexReader.Open(rd, true);

            // warm the cache with the class reader
            Terms t = MockCache.Instance.Get(this.reader, "Cached");
            this.AssertCache(t, 1, "Cached", 10);

            // warm the cache with the method reader
            t = MockCache.Instance.Get(r2, "Cached");
            this.AssertCache(t, 2, "Cached", 10);
        }

        [Test]
        public void GCRemoval()
        {
            // warm the cache
            Terms t = MockCache.Instance.Get(this.reader, "Cached");
            this.AssertCache(t, 1, "Cached", 10);

            // add items to the existing index
            this.AddItemsToIndex(this.directory);
            IndexReader newReader = IndexReader.Open(this.directory, true);
            Assert.AreEqual(20, newReader.NumDocs());

            // test the cache, the old item from the class reader should still be there
            t = MockCache.Instance.Get(newReader, "Cached");
            this.AssertCache(t, 2, "Cached", 20);

            // close and null out the class reader, then force a GC
            this.reader.Close();
            this.reader = null;
            GC.Collect();

            // test the cache, should still have the item from the method reader
            // as there has been no addition to the cache since it was removed
            t = MockCache.Instance.Get(newReader, "Cached");
            this.AssertCache(t, 2, "Cached", 20);

            // add another reader to the cache, which will clear out the class reader
            IndexReader newReader2 = IndexReader.Open(this.directory, true);
            t = MockCache.Instance.Get(newReader2, "Cached");
            this.AssertCache(t, 2, "Cached", 20);

            newReader.Close();
            newReader = null;
            newReader2.Close();
            newReader2 = null;
        }

        private void AssertCache(Terms t, int keyCount, string field, int count)
        {
            Assert.AreEqual(keyCount, MockCache.Instance.KeyCount);
            Assert.AreEqual(field, t.Field);
            Assert.AreEqual(count, t.Count);
        }

        private void CreateInitialIndex(Directory dir)
        {
            IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);
            this.AddDocuments(writer, 0, 10);
            writer.Close();
        }

        private void AddItemsToIndex(Directory dir)
        {
            IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.UNLIMITED);
            this.AddDocuments(writer, 20, 30);
            writer.Close();
        }

        private void AddDocuments(IndexWriter writer, int start, int end)
        {
            for (int i = start; i < end; i++)
            {
                Document doc = new Document();
                doc.Add(new Field("Cached", i.ToString(), Field.Store.NO, Field.Index.ANALYZED));
                doc.Add(new Field("Skipped", i.ToString(), Field.Store.NO, Field.Index.ANALYZED));
                writer.AddDocument(doc);
            }
        }

        /// <summary>
        /// Mock cache that caches the number of terms in a field.
        /// </summary>
        public class MockCache : SegmentCache<Terms>
        {
            /// <summary>
            /// Singleton.
            /// </summary>
            private static MockCache instance = new MockCache();

            /// <summary>
            /// Singleton accessor.
            /// </summary>
            public static MockCache Instance
            {
                get { return MockCache.instance; }
            }

            /// <summary>
            /// Create the values for the cache.
            /// </summary>
            /// <param name="reader"></param>
            /// <param name="key">The key for the item in cache - in this case a field name.</param>
            /// <returns></returns>
            protected override Terms CreateValue(IndexReader reader, string key)
            {
                Terms item = new Terms();
                item.Field = key;

                using (StringFieldEnumerator sfe = new StringFieldEnumerator(reader, key, false))
                {
                    foreach (string s in sfe.Terms)
                    {
                        item.Count++;
                    }
                }

                return item;
            }
        }


        /// <summary>
        /// Simple item to cache.
        /// </summary>
        public class Terms
        {
            /// <summary>
            /// The name of the field.
            /// </summary>
            public string Field
            {
                get;
                set;
            }

            /// <summary>
            /// The number of terms in a field.
            /// </summary>
            public int Count
            {
                get;
                set;
            }

            /// <summary>
            /// Reset the instance.
            /// </summary>
            public void Reset()
            {
                this.Field = string.Empty;
                this.Count = 0;
            }
        }
    }
}
