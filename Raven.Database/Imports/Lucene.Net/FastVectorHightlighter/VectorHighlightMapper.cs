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

#if LUCENENET_350 //Lucene.Net specific code. See https://issues.apache.org/jira/browse/LUCENENET-350

using System;
using System.Collections.Generic;
using Lucene.Net.Index;

namespace Lucene.Net.Search.Vectorhighlight
{
    public class VectorHighlightMapper : TermVectorMapper, ITermFreqVector, TermPositionVector
    {
        private readonly List<string> _terms;
        private Dictionary<string, TermVectorOffsetInfo[]> _tvoi;
        private Dictionary<string, int[]> _positions;
        private Dictionary<string, int> _frequency;
        private List<string> _indexMap;
        private string _field;
        private bool _storeOffsets;
        private bool _storePositions;

        public VectorHighlightMapper(List<string> terms)
        {
            _terms = terms;
            _tvoi = new Dictionary<string, TermVectorOffsetInfo[]>();
            _positions = new Dictionary<string, int[]>();
            _frequency = new Dictionary<string, int>();
            _indexMap = new List<string>();
        }

        public override void SetExpectations(string field, int numTerms, bool storeOffsets, bool storePositions)
        {
            _field = field;
            _storeOffsets = storeOffsets;
            _storePositions = storePositions;
            if (_storeOffsets)
                _tvoi = new Dictionary<string, TermVectorOffsetInfo[]>(numTerms);
            if (_storePositions)
                _positions = new Dictionary<string, int[]>(numTerms);
            _frequency = new Dictionary<string, int>(numTerms);
            _indexMap = new List<string>(numTerms);
        }

        public override void Map(string term, int frequency, TermVectorOffsetInfo[] offsets, int[] positions)
        {
            if (StringUtils.AnyTermMatch(_terms, term))
            {
                _indexMap.Add(term);
                if (_storeOffsets)
                    _tvoi.Add(term, offsets);
                if (_storePositions)
                    _positions.Add(term, positions);
                _frequency.Add(term,frequency);
            }
        }

        public string Field
        {
            get { return _field; }
        }

        public int Size
        {
            get { return _tvoi.Count; }
        }

        public string[] GetTerms()
        {
            string[] result = new string[_tvoi.Count];
            _tvoi.Keys.CopyTo(result,0);
            return result;
        }

        public int[] GetTermFrequencies()
        {
            int[] result = new int[_frequency.Count];
            _frequency.Values.CopyTo(result,0);
            return result;
        }

        public int IndexOf(string term)
        {
            return _indexMap.IndexOf(term);
        }

        public int[] IndexesOf(string[] terms, int start, int len)
        {
            int[] result = new int[terms.Length];
            for (int i = 0; i < terms.Length; i++)
            {
                string term = terms[i];
                result[i] = _indexMap.IndexOf(term, start, len);
            }
            return result;
        }

        public int[] GetTermPositions(int index)
        {
            if (index<_positions.Count)
            {
                string key = _indexMap[index];
                return _positions[key];
            }
            return new int[0];
        }

        public TermVectorOffsetInfo[] GetOffsets(int index)
        {
            if (index < _tvoi.Count)
            {
                string key = _indexMap[index];
                return _tvoi[key];
            }
            return new TermVectorOffsetInfo[0];
        }
    }
}

#endif