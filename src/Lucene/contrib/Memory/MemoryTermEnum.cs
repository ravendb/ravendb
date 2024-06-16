/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Lucene.Net.Index.Memory
{
    public partial class MemoryIndex
    {
        private sealed partial class MemoryIndexReader
        {
            private class MemoryTermEnum : TermEnum
            {
                private readonly MemoryIndex _index;
                private readonly MemoryIndexReader _reader;
                private int _i; // index into info.sortedTerms
                private int _j; // index into sortedFields

                public MemoryTermEnum(MemoryIndex index, MemoryIndexReader reader, int ix, int jx)
                {
                    _index = index;
                    _reader = reader;
                    _i = ix; // index into info.sortedTerms
                    _j = jx; // index into sortedFields
                }

                public override bool Next()
                {
                    if (DEBUG) System.Diagnostics.Debug.WriteLine("TermEnum.next");
                    if (_j >= _index.sortedFields.Length) return false;
                    Info info = _reader.GetInfo(_j);
                    if (++_i < info.SortedTerms.Length) return true;

                    // move to successor
                    _j++;
                    _i = 0;
                    if (_j >= _index.sortedFields.Length) return false;
                    _reader.GetInfo(_j).SortTerms();
                    return true;
                }

                public override Term Term
                {
                    get
                    {
                        if (DEBUG) System.Diagnostics.Debug.WriteLine("TermEnum.term: " + _i);
                        if (_j >= _index.sortedFields.Length) return null;
                        Info info = _reader.GetInfo(_j);
                        if (_i >= info.SortedTerms.Length) return null;
                        //          if (DEBUG) System.Diagnostics.Debug.WriteLine("TermEnum.term: " + i + ", " + info.sortedTerms[i].getKey());
                        return CreateTerm(info, _j, info.SortedTerms[_i].Key);
                    }
                }

                public override int DocFreq()
                {                
                              if (DEBUG) System.Diagnostics.Debug.WriteLine("TermEnum.docFreq");
                              if (_j >= _index.sortedFields.Length) return 0;
                              Info info = _reader.GetInfo(_j);
                              if (_i >= info.SortedTerms.Length) return 0;
                              return _index.NumPositions(info.GetPositions(_i));
                }

                protected override void Dispose(bool disposing)
                {
                              if (DEBUG) System.Diagnostics.Debug.WriteLine("TermEnum.close");
                }

                private Term CreateTerm(Info info, int pos, string text)
                {
                    // Assertion: sortFields has already been called before
                    Term template = info.template;
                    if (template == null) { // not yet cached?
                        String fieldName = _index.sortedFields[pos].Key;
                    template = new Term(fieldName);
                    info.template = template;
                    }

                    return template.CreateTerm(text);
                }
            }
        }
    }
}
