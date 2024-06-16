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
            private sealed class MemoryTermPositions : TermPositions
            {
                private readonly MemoryIndex _index;
                private readonly MemoryIndexReader _reader;
                private bool hasNext;
                private int cursor = 0;
                private ArrayIntList current;
                private Term term;

                public MemoryTermPositions(MemoryIndex index, MemoryIndexReader reader)
                {
                    _index = index;
                    _reader = reader;
                }

                public void Seek(Term term)
                {
                    this.term = term;

                    if (DEBUG) System.Diagnostics.Debug.WriteLine(".seek: " + term);

                    if (term == null)
                    {
                        hasNext = true; // term==null means match all docs
                    }
                    else
                    {
                        Info info = _reader.GetInfo(term.Field);
                        current = info == null ? null : info.GetPositions(term.Text);
                        hasNext = (current != null);
                        cursor = 0;
                    }
                }

                public void Seek(TermEnum termEnum)
                {
                    if (DEBUG) System.Diagnostics.Debug.WriteLine(".seekEnum");
                    Seek(termEnum.Term);
                }

                public int Doc
                {
                    get
                    {
                        if (DEBUG) System.Diagnostics.Debug.WriteLine(".doc");
                        return 0;
                    }
                }

                public int Freq
                {
                    get
                    {
                        int freq = current != null ? _index.NumPositions(current) : (term == null ? 1 : 0);
                        if (DEBUG) System.Diagnostics.Debug.WriteLine(".freq: " + freq);
                        return freq;
                    }
                }

                public bool Next()
                {
                    if (DEBUG) System.Diagnostics.Debug.WriteLine(".next: " + current + ", oldHasNext=" + hasNext);
                    bool next = hasNext;
                    hasNext = false;
                    return next;
                }

                public int Read(int[] docs, int[] freqs)
                {
                    if (DEBUG) System.Diagnostics.Debug.WriteLine(".read: " + docs.Length);
                    if (!hasNext) return 0;
                    hasNext = false;
                    docs[0] = 0;
                    freqs[0] = Freq;
                    return 1;
                }

                public bool SkipTo(int target)
                {
                    if (DEBUG) System.Diagnostics.Debug.WriteLine(".skipTo: " + target);
                    return Next();
                }

                public void Close()
                {
                    if (DEBUG) System.Diagnostics.Debug.WriteLine(".close");
                }

                public void Dispose()
                {
                    if (DEBUG) System.Diagnostics.Debug.WriteLine(".close");
                }

                public int NextPosition()
                {
                    int pos = current.Get(cursor);
                    cursor += _index.stride;
                    if (DEBUG) System.Diagnostics.Debug.WriteLine(".nextPosition: " + pos);
                    return pos;
                }

                public int PayloadLength
                {
                    get { throw new NotSupportedException(); }
                }

                public byte[] GetPayload(byte[] data, int offset)
                {
                    throw new NotSupportedException();
                }

                public bool IsPayloadAvailable
                {
                    get { return false; }
                }
            }
        }
    }
}
