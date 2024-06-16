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

using System.Collections.Generic;

namespace Lucene.Net.Analysis.Shingle.Matrix
{
    /// <summary>
    /// A column focused matrix in three dimensions:
    /// 
    /// <pre>
    /// Token[column][row][z-axis] {
    ///     {{hello}, {greetings, and, salutations}},
    ///     {{world}, {earth}, {tellus}}
    /// };
    /// </pre>
    /// 
    /// todo consider row groups
    /// to indicate that shingles is only to contain permutations with texts in that same row group.
    /// </summary>
    public class Matrix
    {
        public Matrix()
        {
            Columns = new List<Column>();
        }

        public List<Column> Columns { get; private set; }

        public MatrixPermutationIterator PermutationIterator()
        {
            return new MatrixPermutationIterator(this);
        }

        public override string ToString()
        {
            return "Matrix{" +
                   "columns=" + Columns +
                   '}';
        }
    }
}