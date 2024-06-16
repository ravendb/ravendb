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

using Lucene.Net.Support;

namespace Lucene.Net.Analysis.Payloads
{
    /// <summary>
    /// Utility methods for encoding payloads.
    /// </summary>
    public static class PayloadHelper
    {
        public static byte[] EncodeFloat(float payload)
        {
            return EncodeFloat(payload, new byte[4], 0);
        }

        public static byte[] EncodeFloat(float payload, byte[] data, int offset)
        {
            return EncodeInt(Single.FloatToIntBits(payload), data, offset);
        }

        public static byte[] EncodeInt(int payload)
        {
            return EncodeInt(payload, new byte[4], 0);
        }

        public static byte[] EncodeInt(int payload, byte[] data, int offset)
        {
            data[offset] = (byte) (payload >> 24);
            data[offset + 1] = (byte) (payload >> 16);
            data[offset + 2] = (byte) (payload >> 8);
            data[offset + 3] = (byte) payload;
            return data;
        }

        /// <summary>
        /// <p>Decode the payload that was encoded using encodeFloat(float)</p>
        /// <p>NOTE: the length of the array must be at least offset + 4 long.</p>
        /// </summary>
        /// <param name="bytes">The bytes to decode</param>
        /// <returns>the decoded float</returns>
        public static float DecodeFloat(byte[] bytes)
        {
            return DecodeFloat(bytes, 0);
        }

        /// <summary>
        /// <p>Decode the payload that was encoded using encodeFloat(float)</p>
        /// <p>NOTE: the length of the array must be at least offset + 4 long.</p>
        /// </summary>
        /// <param name="bytes">The bytes to decode</param>
        /// <param name="offset">The offset into the array.</param>
        /// <returns>The float that was encoded</returns>
        public static float DecodeFloat(byte[] bytes, int offset)
        {
            return Single.IntBitsToFloat(DecodeInt(bytes, offset));
        }

        public static int DecodeInt(byte[] bytes, int offset)
        {
            return ((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16)
                   | ((bytes[offset + 2] & 0xFF) << 8) | (bytes[offset + 3] & 0xFF);
        }
    }
}