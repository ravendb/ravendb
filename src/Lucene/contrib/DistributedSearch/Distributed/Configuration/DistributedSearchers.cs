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
using System.Configuration;
using System.Xml;

namespace Lucene.Net.Distributed.Configuration
{
    /// <summary>
    /// Definition of a configurable set of search indexes made accessible by the 
    /// LuceneServer windows service for a consuming application. These search indexes 
    /// are defined in the configuration file of an application. The locations defined 
    /// in a DistributedSearcher match the exposed object URIs as defined in the LuceneServer service.
    /// 
    /// An example configuration would look like the following:
    /// <code>
    ///     <DistributedSearchers>
    ///         <DistributedSearcher wsid="1" SearchMethod="0" location="c:\localindexes\LocalIndex1" />
    ///         <DistributedSearcher wsid="2" SearchMethod="1" location="tcp://192.168.1.100:1089/RemoteIndex1" />
    ///         <DistributedSearcher wsid="3" SearchMethod="1" location="tcp://192.168.1.101:1089/RemoteIndex2" />
    ///     </DistributedSearchers>
    /// </code>
    /// </summary>
    public class DistributedSearchers
	{
		private DistributedSearcher[] _arDistributedSearcherArray;

        /// <summary>
        /// Accessor method for the configurable DistributedSearchers.
        /// </summary>
		public static DistributedSearchers GetConfig
		{
            get { return (DistributedSearchers)ConfigurationManager.GetSection("DistributedSearchers"); }
		}

        /// <summary>
        /// Public constructor for DistributedSearchers. A DistributedSearcher is defined
        /// in XML configuration and is loaded via a custom configuration handler.
        /// </summary>
        /// <param name="xSection">The Xml definition in the configuration file</param>
        public DistributedSearchers(XmlNode xSection)
		{
			this._arDistributedSearcherArray = new DistributedSearcher[xSection.ChildNodes.Count];
			int x=0;

			foreach (XmlNode c in xSection.ChildNodes)
			{
				if (c.Name.ToLower()=="DistributedSearcher")
				{
					DistributedSearcher ws = new DistributedSearcher(c);
					this._arDistributedSearcherArray[x] = ws;
					x++;
				}
			}
		}

        /// <summary>
        /// Strongly-typed array of DistributedSearcher objects as defined in 
        /// a configuration section.
        /// </summary>
        public DistributedSearcher[] DistributedSearcherArray
		{
			get {return this._arDistributedSearcherArray;}
		}

	}
}
