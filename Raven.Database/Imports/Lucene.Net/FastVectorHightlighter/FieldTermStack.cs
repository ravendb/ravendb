/*
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
using System.Collections.Generic;
using System.Text;

using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Search;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Store;


namespace Lucene.Net.Search.Vectorhighlight
{
   
   /// <summary>
   /// <c>FieldTermStack</c> is a stack that keeps query terms in the specified field
   /// of the document to be highlighted.
   /// </summary>
    public class FieldTermStack
    {
        private String fieldName;
        public LinkedList<TermInfo> termList = new LinkedList<TermInfo>();

        public static void Main(String[] args)
        {
            Analyzer analyzer = new WhitespaceAnalyzer();
            QueryParser parser = new QueryParser(Util.Version.LUCENE_CURRENT, "f", analyzer);
            Query query = parser.Parse("a x:b");
            FieldQuery fieldQuery = new FieldQuery(query, true, false);

            Directory dir = new RAMDirectory();
            IndexWriter writer = new IndexWriter(dir, analyzer, IndexWriter.MaxFieldLength.LIMITED);
            Document doc = new Document();
            doc.Add(new Field("f", "a a a b b c a b b c d e f", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
            doc.Add(new Field("f", "b a b a f", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
            writer.AddDocument(doc);
            writer.Close();

            IndexReader reader = IndexReader.Open(dir,true);
            FieldTermStack ftl = new FieldTermStack(reader, 0, "f", fieldQuery);
            reader.Close();
        }

        /// <summary>
        /// a constructor. 
        /// </summary>
        /// <param name="reader">IndexReader of the index</param>
        /// <param name="docId">document id to be highlighted</param>
        /// <param name="fieldName">field of the document to be highlighted</param>
        /// <param name="fieldQuery">FieldQuery object</param>
#if LUCENENET_350 //Lucene.Net specific code. See https://issues.apache.org/jira/browse/LUCENENET-350
        public FieldTermStack(IndexReader reader, int docId, String fieldName, FieldQuery fieldQuery)
        {
            this.fieldName = fieldName;
            
            List<string> termSet = fieldQuery.getTermSet(fieldName);

            // just return to make null snippet if un-matched fieldName specified when fieldMatch == true
            if (termSet == null) return;

            //TermFreqVector tfv = reader.GetTermFreqVector(docId, fieldName);
            VectorHighlightMapper tfv = new VectorHighlightMapper(termSet);    
            reader.GetTermFreqVector(docId, fieldName, tfv);
                
            if (tfv.Size==0) return; // just return to make null snippets
            
            string[] terms = tfv.GetTerms();
            foreach (String term in terms)
            {
                if (!StringUtils.AnyTermMatch(termSet, term)) continue;
                int index = tfv.IndexOf(term);
                TermVectorOffsetInfo[] tvois = tfv.GetOffsets(index);
                if (tvois == null) return; // just return to make null snippets
                int[] poss = tfv.GetTermPositions(index);
                if (poss == null) return; // just return to make null snippets
                for (int i = 0; i < tvois.Length; i++)
                    termList.AddLast(new TermInfo(term, tvois[i].StartOffset, tvois[i].EndOffset, poss[i]));
            }
            // sort by position
            //Collections.sort(termList);
            Sort(termList);
        }
#else   //Original Port
        public FieldTermStack(IndexReader reader, int docId, String fieldName, FieldQuery fieldQuery)
        {
            this.fieldName = fieldName;

            TermFreqVector tfv = reader.GetTermFreqVector(docId, fieldName);
            if (tfv == null) return; // just return to make null snippets
            TermPositionVector tpv = null;
            try
            {
                tpv = (TermPositionVector)tfv;
            }
            catch (InvalidCastException e)
            {
                return; // just return to make null snippets
            }

            List<String> termSet = fieldQuery.getTermSet(fieldName);
            // just return to make null snippet if un-matched fieldName specified when fieldMatch == true
            if (termSet == null) return;

            foreach (String term in tpv.GetTerms())
            {
                if (!termSet.Contains(term)) continue;
                int index = tpv.IndexOf(term);
                TermVectorOffsetInfo[] tvois = tpv.GetOffsets(index);
                if (tvois == null) return; // just return to make null snippets
                int[] poss = tpv.GetTermPositions(index);
                if (poss == null) return; // just return to make null snippets
                for (int i = 0; i < tvois.Length; i++)
                    termList.AddLast(new TermInfo(term, tvois[i].GetStartOffset(), tvois[i].GetEndOffset(), poss[i]));
            }

            // sort by position
            //Collections.sort(termList);
            Sort(termList);
        }
#endif

        void Sort(LinkedList<TermInfo> linkList)
        {
            TermInfo[] arr = new TermInfo[linkList.Count];
            linkList.CopyTo(arr, 0);
            Array.Sort(arr, new Comparison<TermInfo>(PosComparer));

            linkList.Clear();
            foreach (TermInfo t in arr) linkList.AddLast(t);
        }

        int PosComparer(TermInfo t1,TermInfo t2)
        {
            return t1.Position - t2.Position;
        }

       /// <summary>
       /// 
       /// </summary>
       /// <value> field name </value>
       public string FieldName
       {
           get { return fieldName; }
       }

       /// <summary>
        /// 
        /// </summary>
        /// <returns>the top TermInfo object of the stack</returns>
        public TermInfo Pop()
        {
            if (termList.Count == 0) return null;

            LinkedListNode<TermInfo> top =  termList.First;
            termList.RemoveFirst();
            return top.Value;
        }
                
        /// <summary>
        /// 
        /// </summary>
        /// <param name="termInfo">the TermInfo object to be put on the top of the stack</param>
        public void Push(TermInfo termInfo)
        {
            // termList.push( termInfo );  // avoid Java 1.6 feature
            termList.AddFirst(termInfo);
        }

        /// <summary>
        /// to know whether the stack is empty 
        /// </summary>
        /// <returns>true if the stack is empty, false if not</returns>
        public bool IsEmpty()
        {
            return termList == null || termList.Count == 0;
        }

        public class TermInfo : IComparable<TermInfo>
        {

            String text;
            int startOffset;
            int endOffset;
            int position;

            public TermInfo(String text, int startOffset, int endOffset, int position)
            {
                this.text = text;
                this.startOffset = startOffset;
                this.endOffset = endOffset;
                this.position = position;
            }

            public string Text
            {
                get { return text; }
            }

            public int StartOffset
            {
                get { return startOffset; }
            }

            public int EndOffset
            {
                get { return endOffset; }
            }

            public int Position
            {
                get { return position; }
            }

            public override string ToString()
            {
                StringBuilder sb = new StringBuilder();
                sb.Append(text).Append('(').Append(startOffset).Append(',').Append(endOffset).Append(',').Append(position).Append(')');
                return sb.ToString();
            }

            public int CompareTo(TermInfo o)
            {
                return (this.position - o.position);
            }
        }
    }
}
