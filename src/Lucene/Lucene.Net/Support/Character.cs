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
    /// Mimics Java's Character class.
    /// </summary>
    public class Character
    {
        private const char charNull = '\0';
        private const char charZero = '0';
        private const char charA = 'a';

        /// <summary>
        /// </summary>
        public static int MAX_RADIX
        {
            get
            {
                return 36;
            }
        }

        /// <summary>
        /// </summary>
        public static int MIN_RADIX
        {
            get
            {
                return 2;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="digit"></param>
        /// <param name="radix"></param>
        /// <returns></returns>
        public static char ForDigit(int digit, int radix)
        {
            // if radix or digit is out of range,
            // return the null character.
            if (radix < Character.MIN_RADIX)
                return charNull;
            if (radix > Character.MAX_RADIX)
                return charNull;
            if (digit < 0)
                return charNull;
            if (digit >= radix)
                return charNull;

            // if digit is less than 10,
            // return '0' plus digit
            if (digit < 10)
                return (char)((int)charZero + digit);

            // otherwise, return 'a' plus digit.
            return (char)((int)charA + digit - 10);
        }
    }
}
