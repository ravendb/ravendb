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

using Lucene.Net.Search;
using Spatial4n.Shapes;

namespace Lucene.Net.Spatial.BBox
{
    /// <summary>
    /// Abstraction of the calculation used to determine how similar two Bounding Boxes are.
    /// </summary>
	public interface BBoxSimilarity
    {
        double Score(IRectangle extent, Explanation exp);
    }
}