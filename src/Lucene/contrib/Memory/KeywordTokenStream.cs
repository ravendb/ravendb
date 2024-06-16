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
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;

namespace Lucene.Net.Index.Memory
{
    public partial class MemoryIndex
    {
        private sealed class KeywordTokenStream<T> : TokenStream
        {
            private IEnumerator<T> iter;
            private int start = 0;
            private ITermAttribute termAtt;
            private IOffsetAttribute offsetAtt;

            public KeywordTokenStream(IEnumerable<T> keywords)
            {
                iter = keywords.GetEnumerator();
                termAtt = AddAttribute<ITermAttribute>();
                offsetAtt = AddAttribute<IOffsetAttribute>();
            }

            public override bool IncrementToken()
            {
                if (!iter.MoveNext()) return false;

                T obj = iter.Current;
                if (obj == null)
                    throw new ArgumentException("keyword must not be null");

                String term = obj.ToString();
                ClearAttributes();
                termAtt.SetTermBuffer(term);
                offsetAtt.SetOffset(start, start + termAtt.TermLength());
                start += term.Length + 1; // separate words by 1 (blank) character
                return true;
            }

            protected override void Dispose(bool disposing)
            {
            }
        }
    }
}
