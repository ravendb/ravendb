/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;

using NUnit.Framework;

using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using OpenBitSet = Lucene.Net.Util.OpenBitSet;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
using Lucene.Net.Store;

namespace Lucene.Net.Search
{
	
	
	/// <summary> </summary>
    [TestFixture]
	public class TestFilteredSearch:LuceneTestCase
	{

        public TestFilteredSearch(): base("")
        {
        }

		private const System.String FIELD = "category";
		
		[Test]
		public virtual void  TestFilteredSearch_Renamed()
		{
            bool enforceSingleSegment = true;
            RAMDirectory directory = new RAMDirectory();
            int[] filterBits = { 1, 36 };
            SimpleDocIdSetFilter filter = new SimpleDocIdSetFilter(filterBits);
            IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
            SearchFiltered(writer, directory, filter, enforceSingleSegment);
            // run the test on more than one segment
            enforceSingleSegment = false;
            // reset - it is stateful
            filter.Reset();
            writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
            // we index 60 docs - this will create 6 segments
            writer.SetMaxBufferedDocs(10);
            SearchFiltered(writer, directory, filter, enforceSingleSegment);
		}


        public void SearchFiltered(IndexWriter writer, Directory directory, Filter filter, bool optimize)
        {
            try
            {
                for (int i = 0; i < 60; i++)
                {//Simple docs
                    Document doc = new Document();
                    doc.Add(new Field(FIELD, i.ToString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
                    writer.AddDocument(doc);
                }
                if (optimize)
                    writer.Optimize();
                writer.Close();

                BooleanQuery booleanQuery = new BooleanQuery();
                booleanQuery.Add(new TermQuery(new Term(FIELD, "36")), BooleanClause.Occur.SHOULD);


                IndexSearcher indexSearcher = new IndexSearcher(directory);
                ScoreDoc[] hits = indexSearcher.Search(booleanQuery, filter, 1000).scoreDocs;
                Assert.AreEqual(1, hits.Length, "Number of matched documents");

            }
            catch (System.IO.IOException e)
            {
                Assert.Fail(e.Message);
            }

        }
		

        [Serializable]
        public sealed class SimpleDocIdSetFilter : Filter
        {
            private int docBase;
            private int[] docs;
            private int index;
            public SimpleDocIdSetFilter(int[] docs)
            {
                this.docs = docs;
            }
            public override DocIdSet GetDocIdSet(IndexReader reader)
            {
                OpenBitSet set = new OpenBitSet();
                int limit = docBase + reader.MaxDoc();
                for (; index < docs.Length; index++)
                {
                    int docId = docs[index];
                    if (docId > limit)
                        break;
                    set.Set(docId - docBase);
                }
                docBase = limit;
                return set.IsEmpty() ? null : set;
            }

            public void Reset()
            {
                index = 0;
                docBase = 0;
            }
        }
	}
}