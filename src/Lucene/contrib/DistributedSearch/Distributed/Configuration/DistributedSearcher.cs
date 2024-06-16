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
using System.Collections;
using System.Configuration;
using System.IO;
using System.Xml;
using Lucene.Net.Distributed;

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
    ///         <DistributedSearcher id="1" SearchMethod="0" location="c:\localindexes\LocalIndex1" />
    ///         <DistributedSearcher id="2" SearchMethod="1" location="tcp://192.168.1.100:1089/RemoteIndex1" />
    ///         <DistributedSearcher id="3" SearchMethod="1" location="tcp://192.168.1.101:1089/RemoteIndex2" />
    ///     </DistributedSearchers>
    /// </code>
    /// </summary>
    public class DistributedSearcher
	{
        private int _id;
		private SearchMethod _eSearchMethod;
		private string _strLocation;

        /// <summary>
        /// Public constructor for DistributedSearcher. A DistributedSearcher is defined
        /// in XML configuration and is loaded via a custom configuration handler.
        /// </summary>
        /// <param name="xSection">The Xml definition in the configuration file</param>
		public DistributedSearcher(XmlNode xSection)
		{
            
            XmlAttributeCollection attributeCollection = xSection.Attributes;
            if (attributeCollection == null)
                throw new ConfigurationErrorsException("xSection.Attributes invalid: " + Environment.NewLine + xSection.OuterXml);

            try
            {
                this._id = Convert.ToInt32(attributeCollection["id"].Value);
            }
            catch (Exception e)
            {
                throw new ConfigurationErrorsException("DistributedSearcher.id invalid: " + Environment.NewLine + xSection.OuterXml + Environment.NewLine + e.Message);
            }

            try
            {
                this._eSearchMethod = (SearchMethod)Enum.Parse(typeof(SearchMethod), attributeCollection["SearchMethod"].Value);
            }
            catch (Exception)
            {
                throw new ConfigurationErrorsException("DistributedSearcher.SearchMethod invalid: " + Environment.NewLine + xSection.OuterXml);
            }

            try
            {
                this._strLocation = attributeCollection["Location"].Value;
            }
            catch (Exception)
            {
                throw new ConfigurationErrorsException("DistributedSearcher.Location invalid: " + Environment.NewLine + xSection.OuterXml);
            }

            if (this.SearchMethod == SearchMethod.Local)
            {
                //check for file-system existence
                if (!Lucene.Net.Index.IndexReader.IndexExists(this.Location))
                    throw new ConfigurationErrorsException("DistributedSearcher.Location not an index: " + Environment.NewLine + this.Location);
            }
            else if (this.SearchMethod == SearchMethod.Distributed)
            {
                //exec ping check if needed
            }

		}

        /// <summary>
        /// Unique Id value assigned to this DistributedSearcher. Not required for any processing,
        /// simply for identification in reference.
        /// </summary>
        public int Id
        {
            get { return this._id; }
        }

        /// <summary>
        /// Enumeration value specifying the locality of the index -- local or remote
        /// </summary>
		public SearchMethod SearchMethod
		{
			get {return this._eSearchMethod;}
		}
        /// <summary>
        /// Reference path to the DistributedSearcher. If SearchMethod is Local, this is a local
        /// file-system path, i.e. "c:\local\index". If SearchMethod is Distributed, this is the 
        /// URI of the server-activated service type, i.e. "tcp://192.168.1.100:1089/RemoteIndex".
        /// </summary>
		public string Location
		{
			get {return this._strLocation;}
		}
	}
}
