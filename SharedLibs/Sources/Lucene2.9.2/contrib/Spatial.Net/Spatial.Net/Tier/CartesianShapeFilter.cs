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

using System.Collections.Generic;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Util;

namespace Lucene.Net.Spatial.Tier
{
	public class CartesianShapeFilter : Filter
	{
		private readonly Shape _shape;
		private readonly string _fieldName;

		public CartesianShapeFilter(Shape shape, string fieldName)
		{
			_shape = shape;
			_fieldName = fieldName;
		}

		public override DocIdSet GetDocIdSet(IndexReader reader)
		{
			var bits = new OpenBitSet(reader.MaxDoc());

			TermDocs termDocs = reader.TermDocs();
			List<double> area = _shape.Area;
			int sz = area.Count;
			
			// iterate through each boxid
			for (int i = 0; i < sz; i++)
			{
				double boxId = area[i];
				termDocs.Seek(new Term(_fieldName, NumericUtils.DoubleToPrefixCoded(boxId)));

				// iterate through all documents
				// which have this boxId
				while (termDocs.Next())
				{
					bits.FastSet(termDocs.Doc());
				}
			}

			return bits;
		}
	}
}