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

namespace Lucene.Net.Analysis.Shingle.Matrix
{
    public class MatrixPermutationIterator
    {
        private readonly Analysis.Shingle.Matrix.Matrix _enclosingInstance;

        private readonly int[] _columnRowCounters;

        public MatrixPermutationIterator(Analysis.Shingle.Matrix.Matrix enclosingInstance)
        {
            _enclosingInstance = enclosingInstance;
            _columnRowCounters = new int[_enclosingInstance.Columns.Count];
        }

        public bool HasNext()
        {
            var s = _columnRowCounters.Length;
            var n = _enclosingInstance.Columns.Count;
            return s != 0 && n >= s && _columnRowCounters[s - 1] < _enclosingInstance.Columns[s - 1].Rows.Count;
        }

        public Row[] Next()
        {
            if (!HasNext())
                throw new Exception("no more elements");

            var rows = new Row[_columnRowCounters.Length];

            for (int i = 0; i < _columnRowCounters.Length; i++)
            {
                rows[i] = _enclosingInstance.Columns[i].Rows[_columnRowCounters[i]];
            }

            IncrementColumnRowCounters();

            return rows;
        }

        private void IncrementColumnRowCounters()
        {
            for (int i = 0; i < _columnRowCounters.Length; i++)
            {
                _columnRowCounters[i]++;

                if (_columnRowCounters[i] != _enclosingInstance.Columns[i].Rows.Count ||
                    i >= _columnRowCounters.Length - 1)
                    break;

                _columnRowCounters[i] = 0;
            }
        }
    }
}