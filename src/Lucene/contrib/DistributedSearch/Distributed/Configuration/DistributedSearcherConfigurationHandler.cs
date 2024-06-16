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
    /// Implementation of custom configuration handler for the definition of search indexes
    /// made accessible by the LuceneServer windows service. This configuration resides in
    /// the configuration file of an application consuming the search indexes made accessible
    /// by the LuceneServer windows service.
    /// </summary>
    public class DistributedSearcherConfigurationHandler : IConfigurationSectionHandler
	{
        /// <summary>
        /// Empty public constructor for the configuration handler.
        /// </summary>
		public DistributedSearcherConfigurationHandler()
		{
		}

		#region IConfigurationSectionHandler Members

        /// <summary>
        /// Required implementation of IConfigurationSectionHandler.
        /// </summary>
        /// <param name="parent">Required object for IConfigurationSectionHandler</param>
        /// <param name="configContext">Configuration context object</param>
        /// <param name="section">Xml configuration in the application configuration file</param>
        /// <returns></returns>
		public object Create(object parent, object configContext, XmlNode section)
		{
			DistributedSearchers wsConfig = new DistributedSearchers(section);
			return wsConfig;
		}

		#endregion
	}
}
