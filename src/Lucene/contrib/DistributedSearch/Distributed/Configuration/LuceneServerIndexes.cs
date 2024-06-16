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
    /// Definition of configurable search indexes made accessible by the 
    /// LuceneServer windows service.
    /// 
    /// An example configuration would look like the following:
    /// <code>
    ///   <LuceneServerIndexes Port="1089">
    ///     <LuceneServerIndex ObjectUri="RemoteIndexes1">
    ///         <Directory indexA="c:\lucene\indexA\index1" indexB="c:\lucene\indexB\index1" />
    ///         <Directory indexA="c:\lucene\indexA\index2" indexB="c:\lucene\indexB\index2" />
    ///     </LuceneServerIndex>
    ///     <LuceneServerIndex ObjectUri="RemoteIndexes2">
    ///         <Directory indexA="c:\lucene\indexA\index3" indexB="c:\lucene\indexB\index3" />
    ///         <Directory indexA="c:\lucene\indexA\index4" indexB="c:\lucene\indexB\index4" />
    ///     </LuceneServerIndex>
    ///   </LuceneServerIndexes>
    /// </code>
    /// </summary>
    public class LuceneServerIndexes
	{
		private LuceneServerIndex[] _arLuceneServerIndexArray;
		private int _intPort;

        /// <summary>
        /// Accessor method for the configurable search indexes.
        /// </summary>
		public static LuceneServerIndexes GetConfig
		{
			get {return (LuceneServerIndexes)ConfigurationManager.GetSection("LuceneServerIndexes");}
		}

        /// <summary>
        /// Public constructor for LuceneServerIndexes. A LuceneServerIndex is defined
        /// in XML configuration and is loaded via a custom configuration handler.
        /// </summary>
        /// <param name="xSection">The Xml definition in the configuration file</param>
        public LuceneServerIndexes(XmlNode xSection)
		{
			XmlAttributeCollection attributeCollection = xSection.Attributes;

            try
            {
                this._intPort = Convert.ToInt32(attributeCollection["Port"].Value);
            }
            catch (Exception)
            {
                throw new ConfigurationErrorsException("LuceneServerIndexes port definition invalid: "+Environment.NewLine+xSection.OuterXml);
            }

            if (xSection.ChildNodes.Count==0)
                throw new ConfigurationErrorsException("LuceneServerIndexes configuration missing: " + Environment.NewLine + xSection.OuterXml);

			this._arLuceneServerIndexArray = new LuceneServerIndex[xSection.ChildNodes.Count];
			int x=0;

			foreach (XmlNode c in xSection.ChildNodes)
			{
				if (c.Name.ToLower()=="luceneserverindex")
				{
					LuceneServerIndex rs = new LuceneServerIndex(c, _intPort);
					this._arLuceneServerIndexArray[x] = rs;
					x++;
				}

			}
		}

        /// <summary>
        /// Strongly-typed array of LuceneServerIndex objects as defined in 
        /// a configuration section.
        /// </summary>
		public LuceneServerIndex[] LuceneServerIndexArray
		{
			get {return this._arLuceneServerIndexArray;}
		}

        /// <summary>
        /// A default Port to be assigned to all defined LuceneServerIndex objects.
        /// This value can be overridden for a specific LuceneServerIndex.
        /// </summary>
		public int Port
		{
			get {return this._intPort;}
		}
	}
}
