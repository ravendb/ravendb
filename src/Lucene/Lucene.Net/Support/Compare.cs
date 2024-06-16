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
    /// Summary description for TestSupportClass.
    /// </summary>
    public class Compare
    {
        /// <summary>
        /// Compares two Term arrays for equality.
        /// </summary>
        /// <param name="t1">First Term array to compare</param>
        /// <param name="t2">Second Term array to compare</param>
        /// <returns>true if the Terms are equal in both arrays, false otherwise</returns>
        public static bool CompareTermArrays(Index.Term[] t1, Index.Term[] t2)
        {
            if (t1.Length != t2.Length)
                return false;
            for (int i = 0; i < t1.Length; i++)
            {
                if (t1[i].CompareTo(t2[i]) == 0)
                {
                    return true;
                }
            }
            return false;
        }
    }
}
