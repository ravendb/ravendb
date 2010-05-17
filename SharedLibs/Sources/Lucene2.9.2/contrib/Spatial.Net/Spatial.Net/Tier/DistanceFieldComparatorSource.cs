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
using Lucene.Net.Search;

namespace Lucene.Net.Spatial.Tier
{
	public class DistanceFieldComparatorSource : FieldComparatorSource
	{
		private readonly DistanceFilter _distanceFilter;
		private DistanceScoreDocLookupComparator _dsdlc;

		public DistanceFieldComparatorSource(DistanceFilter distanceFilter)
		{
			this._distanceFilter = distanceFilter;
		}

		public override FieldComparator NewComparator(string fieldname, int numHits, int sortPos, bool reversed)
		{
			return _dsdlc = new DistanceScoreDocLookupComparator(_distanceFilter, numHits);
			
		}

		private class DistanceScoreDocLookupComparator : FieldComparator
		{
			private DistanceFilter _distanceFilter;
			private readonly double[] _values;
			private double _bottom;
			private int _offset = 0;

			public DistanceScoreDocLookupComparator(DistanceFilter distanceFilter, int numHits)
			{
				this._distanceFilter = distanceFilter;
				_values = new double[numHits];
				return;
			}

			public override int Compare(int slot1, int slot2)
			{
				double a = _values[slot1];
				double b = _values[slot2];
				if (a > b)
					return 1;
				if (a < b)
					return -1;

				return 0;
			}

			public override int CompareBottom(int doc)
			{
				double v2 = _distanceFilter.GetDistance(doc + _offset);

				if (_bottom > v2)
				{
					return 1;
				}
				
				if (_bottom < v2)
				{
					return -1;
				}
				
				return 0;
			}

			public override void Copy(int slot, int doc)
			{
				_values[slot] = _distanceFilter.GetDistance(doc + _offset);
			}

			public override void SetBottom(int slot)
			{
				this._bottom = _values[slot];
			}

			public override void SetNextReader(Lucene.Net.Index.IndexReader reader, int docBase)
			{
				// each reader in a segmented base
				// has an offset based on the maxDocs of previous readers
				_offset = docBase;
			}

			public override IComparable Value(int slot)
			{
				return _values[slot];
			}

			public void CleanUp()
			{
				_distanceFilter = null;
			}
		}
	}
}
