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
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.QueryParsers;
using Lucene.Net.Store;
using Lucene.Net.Util;

namespace Lucene.Net.Search
{
    public partial class SimpleFacetedSearch
    {
        public class HitsPerFacet : IEnumerable<Document>, IEnumerator<Document>
        {
            IndexReader _Reader;
            int _MaxDocPerFacet;
            int _ItemsReturned = 0;
            DocIdSetIterator _ResultIterator;
            OpenBitSet _ResultBitSet;
            int _CurrentDocId;
            DocIdSet _QueryDocidSet;
            OpenBitSetDISI _GroupBitSet;

            FacetName _FacetName;
            long _HitCount = -1;

            internal HitsPerFacet(FacetName facetName, IndexReader reader, DocIdSet queryDocidSet, OpenBitSetDISI groupBitSet, int maxDocPerFacet)
            {
                this._FacetName = facetName;
                this._Reader = reader;
                this._MaxDocPerFacet = maxDocPerFacet;
                this._QueryDocidSet = queryDocidSet;
                this._GroupBitSet = groupBitSet;
                
            }

            internal void Calculate()
            {
                if (_QueryDocidSet == DocIdBitSet.EMPTY_DOCIDSET)
                {
                    _ResultBitSet = new OpenBitSet(0);
                }
                else
                {
                    _ResultBitSet = (OpenBitSet)((OpenBitSet)_QueryDocidSet).Clone();
                    _ResultBitSet.And(_GroupBitSet);
                }
                
                _ResultIterator = _ResultBitSet.Iterator();

                _HitCount = _ResultBitSet.Cardinality();

                _ResultBitSet = null;
                _QueryDocidSet = null;
                _GroupBitSet = null;
            }

            public FacetName Name
            {
                get { return _FacetName; }
            }

            public long HitCount
            {
                get{ return _HitCount; }
            }

            public Document Current
            {
                get { return _Reader.Document(_CurrentDocId); }
            }

            object System.Collections.IEnumerator.Current
            {
                get { return _Reader.Document(_CurrentDocId); }
            }

            public bool MoveNext()
            {
                _CurrentDocId = _ResultIterator.NextDoc();
                return _CurrentDocId != DocIdSetIterator.NO_MORE_DOCS && ++_ItemsReturned <= _MaxDocPerFacet;
            }

            public IEnumerator<Document> GetEnumerator()
            {
                return this;
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return this;
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            public void Dispose()
            {

            }

            public HitsPerFacet Documents
            {
                get { return this; }
            }
        }
    }
}
