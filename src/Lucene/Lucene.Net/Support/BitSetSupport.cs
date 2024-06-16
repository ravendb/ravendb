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

namespace Lucene.Net.Support
{
    /// <summary>
    /// This class provides supporting methods of java.util.BitSet
    /// that are not present in System.Collections.BitArray.
    /// </summary>
    public class BitSetSupport
    {
        /// <summary>
        /// Returns the next set bit at or after index, or -1 if no such bit exists.
        /// </summary>
        /// <param name="bitArray"></param>
        /// <param name="index">the index of bit array at which to start checking</param>
        /// <returns>the next set bit or -1</returns>
        public static int NextSetBit(System.Collections.BitArray bitArray, int index)
        {
            while (index < bitArray.Length)
            {
                // if index bit is set, return it
                // otherwise check next index bit
                if (bitArray.Get(index))
                    return index;
                else
                    index++;
            }
            // if no bits are set at or after index, return -1
            return -1;
        }

        /// <summary>
        /// Returns the next un-set bit at or after index, or -1 if no such bit exists.
        /// </summary>
        /// <param name="bitArray"></param>
        /// <param name="index">the index of bit array at which to start checking</param>
        /// <returns>the next set bit or -1</returns>
        public static int NextClearBit(System.Collections.BitArray bitArray, int index)
        {
            while (index < bitArray.Length)
            {
                // if index bit is not set, return it
                // otherwise check next index bit
                if (!bitArray.Get(index))
                    return index;
                else
                    index++;
            }
            // if no bits are set at or after index, return -1
            return -1;
        }

        /// <summary>
        /// Returns the number of bits set to true in this BitSet.
        /// </summary>
        /// <param name="bits">The BitArray object.</param>
        /// <returns>The number of bits set to true in this BitSet.</returns>
        public static int Cardinality(System.Collections.BitArray bits)
        {
            int count = 0;
            for (int i = 0; i < bits.Length; i++)
            {
                if (bits[i])
                    count++;
            }
            return count;
        }
    }
}
