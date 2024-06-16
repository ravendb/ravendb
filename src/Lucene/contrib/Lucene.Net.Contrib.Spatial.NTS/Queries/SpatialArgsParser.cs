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
using System.Collections.Generic;
using System.Text;
using Spatial4n.Context;
using Spatial4n.IO;
using Spatial4n.Shapes;

namespace Lucene.Net.Spatial.Queries
{
	public class SpatialArgsParser
	{
        public const String DIST_ERR_PCT = "distErrPct";
        public const String DIST_ERR = "distErr";

        /// <summary>
        /// Writes a close approximation to the parsed input format.
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public static String WriteSpatialArgs(SpatialArgs args)
        {
            var str = new StringBuilder();
            str.Append(args.Operation.GetName());
            str.Append('(');
            str.Append(args.Shape);
            if (args.DistErrPct != null)
                str.Append(" distErrPct=").Append(String.Format("{0:0.00}%", args.DistErrPct*100d));
            if (args.DistErr != null)
                str.Append(" distErr=").Append(args.DistErr);
            str.Append(')');
            return str.ToString();
        }

        /// <summary>
        /// Parses a string such as "Intersects(-10,20,-8,22) distErrPct=0.025".
        /// </summary>
        /// <param name="v"></param>
        /// <param name="ctx"></param>
        /// <returns></returns>
	    public SpatialArgs Parse(String v, SpatialContext ctx)
		{
			int idx = v.IndexOf('(');
			int edx = v.LastIndexOf(')');

			if (idx < 0 || idx > edx)
			{
                throw new ArgumentException("missing parens: " + v);
			}

			SpatialOperation op = SpatialOperation.Get(v.Substring(0, idx).Trim());

			//Substring in .NET is (startPosn, length), But in Java it's (startPosn, endPosn)
			//see http://docs.oracle.com/javase/1.4.2/docs/api/java/lang/String.html#substring(int, int)
			String body = v.Substring(idx + 1, edx - (idx + 1)).Trim();
			if (body.Length < 1)
			{
				throw new ArgumentException("missing body : " + v);
			}

			IShape shape = ctx.ReadShape(body);
			var args = new SpatialArgs(op, shape);

			if (v.Length > (edx + 1))
			{
				body = v.Substring(edx + 1).Trim();
				if (body.Length > 0)
				{
					Dictionary<String, String> aa = ParseMap(body);
                    args.DistErrPct = ReadDouble(aa["distErrPct"]); aa.Remove(DIST_ERR_PCT);
                    args.DistErr = ReadDouble(aa["distErr"]); aa.Remove(DIST_ERR);
					if (aa.Count != 0)
					{
						throw new ArgumentException("unused parameters: " + aa);
					}
				}
			}
            args.Validate();
			return args;
		}

		protected static double? ReadDouble(String v)
		{
			double val;
			return double.TryParse(v, out val) ? val : (double?)null;
		}

		protected static bool ReadBool(String v, bool defaultValue)
		{
			bool ret;
			return bool.TryParse(v, out ret) ? ret : defaultValue;
		}

        /// <summary>
        /// Parses "a=b c=d f" (whitespace separated) into name-value pairs. If there
        /// is no '=' as in 'f' above then it's short for f=f.
        /// </summary>
        /// <param name="body"></param>
        /// <returns></returns>
		protected static Dictionary<String, String> ParseMap(String body)
		{
			var map = new Dictionary<String, String>();
			int tokenPos = 0;
			var st = body.Split(new[] {' ', '\n', '\t'}, StringSplitOptions.RemoveEmptyEntries);
			while (tokenPos < st.Length)
			{
				String a = st[tokenPos++];
				int idx = a.IndexOf('=');
				if (idx > 0)
				{
					String k = a.Substring(0, idx);
					String v = a.Substring(idx + 1);
					map[k] = v;
				}
				else
				{
					map[a] = a;
				}
			}
			return map;
		}

	}
}
