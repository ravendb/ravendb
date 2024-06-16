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

namespace Lucene.Net.Util
{
	/// <summary> Use by certain classes to match version compatibility
	/// across releases of Lucene.
    ///  <p/>
    ///  <b>WARNING</b>: When changing the version parameter
    ///  that you supply to components in Lucene, do not simply
    ///  change the version at search-time, but instead also adjust
    ///  your indexing code to match, and re-index.
	/// </summary>
	public enum Version
	{
		/// <summary>Match settings and bugs in Lucene's 2.0 release.</summary>
		LUCENE_20,
		
		/// <summary>Match settings and bugs in Lucene's 2.1 release. </summary>
		LUCENE_21,
		
		/// <summary>Match settings and bugs in Lucene's 2.2 release. </summary>
		LUCENE_22,
		
		/// <summary>Match settings and bugs in Lucene's 2.3 release.</summary>
		LUCENE_23,

        /// <summary>Match settings and bugs in Lucene's 2.4 release.</summary>
		LUCENE_24,

        /// <summary>Match settings and bugs in Lucene's 2.9 release.</summary>
		LUCENE_29,

        /// <summary>
        /// Match settings and bugs in Lucene's 3.0 release.
        /// <para>
        /// Use this to get the latest and greatest settings, bug fixes,
        /// etc, for Lucene.
        /// </para>
        /// </summary>
        LUCENE_30,

        // NOTE: Add new constants for later versions **here** to respect order!
		
        /// <summary>
		/// <p/><b>WARNING</b>: if you use this setting, and then
		/// upgrade to a newer release of Lucene, sizable changes
		/// may happen.  If precise back compatibility is important
		/// then you should instead explicitly specify an actual
		/// version.
        /// If you use this constant then you may need to
        /// <b>re-index all of your documents</b> when upgrading
        /// Lucene, as the way text is indexed may have changed.
        /// Additionally, you may need to <b>re-test your entire
        /// application</b> to ensure it behaves as expected, as
        /// some defaults may have changed and may break functionality
        /// in your application.
		/// </summary>
        [Obsolete("Use an actual version instead.")]
		LUCENE_CURRENT,
	}

    public static class VersionEnumExtensions
    {
		public static bool OnOrAfter(this Version first, Version other)
		{
		    return first.CompareTo(other) >= 0;
		}
    }
}