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

namespace Lucene.Net.Search
{
    public partial class SimpleFacetedSearch
    {
        public class Hits
        {
            long _TotalHitCount = -1;
            HitsPerFacet[] _HitsPerGroup;
            Dictionary<string, HitsPerFacet> _Indexer = new Dictionary<string, HitsPerFacet>();
            
            public HitsPerFacet this[string name]
            {
                get{ return _Indexer[name];}
            }

            public HitsPerFacet this[FacetName name]
            {
                get { return _Indexer[name.ToString()]; }
            }

            public long TotalHitCount
            {
                get
                {
                    if (_TotalHitCount == -1)
                    {
                        _TotalHitCount = 0;
                        foreach (var h in _HitsPerGroup)
                        {
                            _TotalHitCount += h.HitCount;
                        }
                    }
                    return _TotalHitCount;
                }
            }

            public HitsPerFacet[] HitsPerFacet
            {
                get { return _HitsPerGroup; }
                internal set
                {
                    _HitsPerGroup = value;
                    foreach (var h in _HitsPerGroup)
                    {
                        _Indexer.Add(h.Name.ToString(), h);
                    }
                }
            }
        }
    }
}
