using System;
using System.Threading;

namespace Lucene.Net.Support.Lucene4x
{
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

    /// <summary>
    /// NOTE: This was AtomicInteger in the JDK
    /// </summary>
#if FEATURE_SERIALIZABLE
    [Serializable]
#endif
    public class AtomicInt32
    {
        private int value;

        public AtomicInt32()
            : this(0)
        {
        }

        public AtomicInt32(int value_)
        {
            Interlocked.Exchange(ref value, value_);
        }

        public int IncrementAndGet()
        {
            return Interlocked.Increment(ref value);
        }

        public int GetAndIncrement()
        {
            return Interlocked.Increment(ref value) - 1;
        }

        public int DecrementAndGet()
        {
            return Interlocked.Decrement(ref value);
        }

        public int GetAndDecrement()
        {
            return Interlocked.Decrement(ref value) + 1;
        }

        public void Set(int value)
        {
            Interlocked.Exchange(ref this.value, value);
        }

        public int AddAndGet(int value)
        {
            return Interlocked.Add(ref this.value, value);
        }

        public int Get()
        {
            //LUCENE TO-DO read operations atomic in 64 bit
            return value;
        }

        public bool CompareAndSet(int expect, int update)
        {
            int rc = Interlocked.CompareExchange(ref value, update, expect);
            return rc == expect;
        }

        public override string ToString()
        {
            return Get().ToString();
        }
    }
}