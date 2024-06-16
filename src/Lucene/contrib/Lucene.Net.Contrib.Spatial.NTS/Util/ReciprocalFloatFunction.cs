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
using Lucene.Net.Index;
using Lucene.Net.Search.Function;
using Lucene.Net.Store;

namespace Lucene.Net.Spatial.Util
{
    public class ReciprocalFloatFunction : ValueSource
    {
        protected readonly ValueSource source;
        protected readonly float m;
        protected readonly float a;
        protected readonly float b;

        /// <summary>
        /// f(source) = a/(m*float(source)+b)
        /// </summary>
        /// <param name="source"></param>
        /// <param name="m"></param>
        /// <param name="a"></param>
        /// <param name="b"></param>
        public ReciprocalFloatFunction(ValueSource source, float m, float a, float b)
        {
            this.source = source;
            this.m = m;
            this.a = a;
            this.b = b;
        }

        public class FloatDocValues : DocValues
        {
            private readonly ReciprocalFloatFunction _enclosingInstance;
            private readonly DocValues vals;

            public FloatDocValues(ReciprocalFloatFunction enclosingInstance, DocValues vals)
            {
                _enclosingInstance = enclosingInstance;
                this.vals = vals;
            }

            public override float FloatVal(int doc)
            {
                return _enclosingInstance.a / (_enclosingInstance.m * vals.FloatVal(doc) + _enclosingInstance.b);
            }

            public override string ToString(int doc)
            {
                return _enclosingInstance.a + "/("
                       + _enclosingInstance.m + "*float(" + vals.ToString(doc) + ')'
                       + '+' + _enclosingInstance.b + ')';
            }
        }

        public override DocValues GetValues(IndexReader reader, IState state)
        {
            var vals = source.GetValues(reader, state);
            return new FloatDocValues(this, vals);
        }

        public override string Description()
        {
            return a + "/("
                   + m + "*float(" + source.Description() + ")"
                   + "+" + b + ')';
        }

        public override bool Equals(object o)
        {
            if (typeof(ReciprocalFloatFunction) != o.GetType()) return false;
            var other = (ReciprocalFloatFunction)o;
            return this.m == other.m
                   && this.a == other.a
                   && this.b == other.b
                   && this.source.Equals(other.source);
        }

        public override int GetHashCode()
        {
            int h = (int) BitConverter.DoubleToInt64Bits(a) + (int) BitConverter.DoubleToInt64Bits(m);
            h ^= (h << 13) | (int)((uint)h >> 20);
            return h + ((int) BitConverter.DoubleToInt64Bits(b)) + source.GetHashCode();
        }
    }
}
