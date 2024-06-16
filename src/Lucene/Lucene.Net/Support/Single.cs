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
using System.Globalization;

namespace Lucene.Net.Support
{
    /// <summary>
    /// 
    /// </summary>
    public class Single
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="s"></param>
        /// <param name="style"></param>
        /// <param name="provider"></param>
        /// <returns></returns>
        public static System.Single Parse(System.String s, System.Globalization.NumberStyles style, System.IFormatProvider provider)
        {
            if (s.EndsWith("f") || s.EndsWith("F"))
                return System.Single.Parse(s.Substring(0, s.Length - 1), style, provider);
            else
                return System.Single.Parse(s, style, provider);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="s"></param>
        /// <param name="provider"></param>
        /// <returns></returns>
        public static System.Single Parse(System.String s, System.IFormatProvider provider)
        {
            if (s.EndsWith("f") || s.EndsWith("F"))
                return System.Single.Parse(s.Substring(0, s.Length - 1), provider);
            else
                return System.Single.Parse(s, provider);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="s"></param>
        /// <param name="style"></param>
        /// <returns></returns>
        public static System.Single Parse(System.String s, System.Globalization.NumberStyles style)
        {
            if (s.EndsWith("f") || s.EndsWith("F"))
                return System.Single.Parse(s.Substring(0, s.Length - 1), style);
            else
                return System.Single.Parse(s, style);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static System.Single Parse(System.String s)
        {
            if (s.EndsWith("f") || s.EndsWith("F"))
                return System.Single.Parse(s.Substring(0, s.Length - 1).Replace(".", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator));
            else
                return System.Single.Parse(s.Replace(".", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator));
        }

        public static bool TryParse(System.String s, out float f)
        {
            bool ok = false;

            if (s.EndsWith("f") || s.EndsWith("F"))
                ok = System.Single.TryParse(s.Substring(0, s.Length - 1).Replace(".", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator), out f);
            else
                ok = System.Single.TryParse(s.Replace(".", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator), out f);

            return ok;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public static string ToString(float f)
        {
            return f.ToString().Replace(CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator, ".");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="f"></param>
        /// <param name="format"></param>
        /// <returns></returns>
        public static string ToString(float f, string format)
        {
            return f.ToString(format).Replace(CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator, ".");
        }

        public static int FloatToIntBits(float value)
        {
            return BitConverter.ToInt32(BitConverter.GetBytes(value), 0);
        }

        public static float IntBitsToFloat(int value)
        {
            return BitConverter.ToSingle(BitConverter.GetBytes(value), 0);
        }
    }
}
