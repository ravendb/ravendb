/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Contrib.Regex
{
	/// <summary>
	/// Defines basic operations needed by <see cref="RegexQuery"/> for a regular expression implementation.
	/// </summary>
	/// <remarks>http://www.java2s.com/Open-Source/Java-Document/Net/lucene-connector/org/apache/lucene/search/regex/RegexCapabilities.java.htm</remarks>
	public interface IRegexCapabilities
	{
		/// <summary>
		/// Called by the constructor of <see cref="RegexTermEnum"/> allowing implementations to cache 
		/// a compiled version of the regular expression pattern.
		/// </summary>
		/// <param name="pattern">regular expression pattern</param>
		void Compile(string pattern);

		/// <summary>
		/// True on match.
		/// </summary>
		/// <param name="s">text to match</param>
		/// <returns>true on match</returns>
		bool Match(string s);

		/// <summary>
		/// A wise prefix implementation can reduce the term enumeration (and thus performance)
		/// of RegexQuery dramatically.
		/// </summary>
		/// <returns>static non-regex prefix of the pattern last passed to <see cref="Compile"/>.
		///   May return null</returns>
		string Prefix();
	}
}
