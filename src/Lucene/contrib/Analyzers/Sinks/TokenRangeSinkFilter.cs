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
using Lucene.Net.Util;

namespace Lucene.Net.Analysis.Sinks
{
    public class TokenRangeSinkFilter : TeeSinkTokenFilter.SinkFilter
    {
        private int lower;
        private int upper;
        private int count;

        public TokenRangeSinkFilter(int lower, int upper)
        {
            this.lower = lower;
            this.upper = upper;
        }

        public override bool Accept(AttributeSource source)
        {
            try
            {
                if (count >= lower && count < upper)
                {
                    return true;
                }
                return false;
            }
            finally
            {
                count++;
            }
        }

        public override void Reset()
        {
            count = 0;
        }
    }
}
