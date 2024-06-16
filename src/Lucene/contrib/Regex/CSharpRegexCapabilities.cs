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

using System;

namespace Contrib.Regex
{
	/// <summary>
	/// C# Regex based implementation of <see cref="IRegexCapabilities"/>.
	/// </summary>
	/// <remarks>http://www.java2s.com/Open-Source/Java-Document/Net/lucene-connector/org/apache/lucene/search/regex/JavaUtilRegexCapabilities.java.htm</remarks>
	public class CSharpRegexCapabilities : IRegexCapabilities, IEquatable<CSharpRegexCapabilities>
	{
		private System.Text.RegularExpressions.Regex _rPattern;

		/// <summary>
		/// Called by the constructor of <see cref="RegexTermEnum"/> allowing implementations to cache 
		/// a compiled version of the regular expression pattern.
		/// </summary>
		/// <param name="pattern">regular expression pattern</param>
		public void Compile(string pattern)
		{
			_rPattern = new System.Text.RegularExpressions.Regex(pattern, 
				System.Text.RegularExpressions.RegexOptions.Compiled);
		}

		/// <summary>
		/// True on match.
		/// </summary>
		/// <param name="s">text to match</param>
		/// <returns>true on match</returns>
		public bool Match(string s)
		{
			return _rPattern.IsMatch(s);
		}

		/// <summary>
		/// A wise prefix implementation can reduce the term enumeration (and thus performance)
		/// of RegexQuery dramatically.
		/// </summary>
		/// <returns>static non-regex prefix of the pattern last passed to <see cref="IRegexCapabilities.Compile"/>.
		///   May return null</returns>
		public string Prefix()
		{
			return null;
		}

		/// <summary>
		/// Indicates whether the current object is equal to another object of the same type.
		/// </summary>
		/// <returns>
		/// true if the current object is equal to the <paramref name="other"/> parameter; otherwise, false.
		/// </returns>
		/// <param name="other">An object to compare with this object</param>
		public bool Equals(CSharpRegexCapabilities other)
		{
			if (other == null) return false;
			if (this == other) return true;

			if (_rPattern != null ? !_rPattern.Equals(other._rPattern) : other._rPattern != null)
				return false;

			return true;
		}

		public override bool Equals(object obj)
		{
			if (obj as CSharpRegexCapabilities == null) return false;
			return Equals((CSharpRegexCapabilities) obj);
		}

		public override int GetHashCode()
		{
			return (_rPattern != null ? _rPattern.GetHashCode() : 0);
		}
	}
}
