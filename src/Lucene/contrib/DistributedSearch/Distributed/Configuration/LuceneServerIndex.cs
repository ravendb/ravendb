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

namespace Lucene.Net.Distributed.Configuration
{
	/// <summary>
    /// Definition of a configurable search index made accessible by the 
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
	public class LuceneServerIndex
	{
		private string _strObjectUri;
		private int _intPort;
		private DirectoryInfo[] _arIndexADirectories;
		private DirectoryInfo[] _arIndexBDirectories;
		private DirectoryInfo[] _arRefreshDirectories;

        /// <summary>
        /// Public constructor for LuceneServerIndex. A LuceneServerIndex is defined
        /// in XML configuration and is loaded via a custom configuration handler.
        /// </summary>
        /// <param name="xSection">The Xml definition in the configuration file</param>
        /// <param name="defaultPort">The default Port value, as defined in the contained 
        /// LuceneServerIndexes configuration</param>
		public LuceneServerIndex(XmlNode xSection, int defaultPort)
		{
			XmlAttributeCollection attributeCollection = xSection.Attributes;
            try
            {
                this._strObjectUri = attributeCollection["ObjectUri"].Value;
            }
            catch (Exception)
            {
                throw new ConfigurationErrorsException("ObjectUri invalid: "+Environment.NewLine + xSection.OuterXml);
            }

            try
            {
                this._intPort = (attributeCollection["port"] != null ? Convert.ToInt32(attributeCollection["port"].Value) : defaultPort);
            }
            catch (Exception)
            {
                throw new ConfigurationErrorsException("port invalid: " + Environment.NewLine + xSection.OuterXml);
            }

            if (xSection.ChildNodes.Count == 0)
                throw new ConfigurationErrorsException("LuceneServerIndex configuration missing: " + Environment.NewLine + xSection.OuterXml);

			_arIndexADirectories = new DirectoryInfo[xSection.ChildNodes.Count];
			_arIndexBDirectories = new DirectoryInfo[xSection.ChildNodes.Count];
			DirectoryInfo diA;
			DirectoryInfo diB;
			int x=0;

			foreach (XmlNode c in xSection.ChildNodes)
			{
				if (c.Name.ToLower()=="directory")
				{
                    try
                    {
                        diA = new DirectoryInfo(c.Attributes["indexA"].Value);
                        _arIndexADirectories[x] = diA;
                        if (!diA.Exists)
                            throw new DirectoryNotFoundException("Directory not found: indexA=" + c.Attributes["indexA"].Value + Environment.NewLine + xSection.OuterXml);
                    }
                    catch (Exception)
                    {
                        throw new ConfigurationErrorsException("LuceneServerIndex configuration Directory error: indexA=" + c.Attributes["indexA"].Value + Environment.NewLine + xSection.OuterXml);
                    }

                    try
                    {
                        diB = new DirectoryInfo(c.Attributes["indexB"].Value);
                        _arIndexBDirectories[x] = diB;
                        if (!diB.Exists)
                            throw new DirectoryNotFoundException("Directory not found: indexA=" + c.Attributes["indexA"].Value + Environment.NewLine + xSection.OuterXml);
                    }
                    catch (Exception)
                    {
                        throw new ConfigurationErrorsException("LuceneServerIndex configuration Directory error: indexA=" + c.Attributes["indexB"].Value + Environment.NewLine + xSection.OuterXml);
                    }
					x++;
				}
			}
		}

        /// <summary>
        /// The published Uri name for a collective set of indexes. The ObjectUri
        /// is referenced by clients consuming the well-known service type. As an example,
        /// an ObjectUri of "RemoteSearchIndex" on a system located at 192.168.1.100, exposed
        /// on port 1089, would be accessed at "tcp://192.168.1.100:1089/RemoteSearchIndex".
        /// <para>This value is required in configuration.</para>
        /// </summary>
        public string ObjectUri
		{
			get {return this._strObjectUri;}
		}

        /// <summary>
        /// A definable port number for the published Uri. Use this value to override the default
        /// Port setting for all published URIs.
        /// <para>This value is optional in configuration.</para>
        /// </summary>
		public int Port
		{
			get {return this._intPort;}
		}

        /// <summary>
        /// File-system path to the "IndexA" location of the index files.
        /// </summary>
		public DirectoryInfo[] IndexADirectories
		{
			get {return this._arIndexADirectories;}
		}

        /// <summary>
        /// File-system path to the "IndexB" location of the index files.
        /// </summary>
		public DirectoryInfo[] IndexBDirectories
		{
			get {return this._arIndexBDirectories;}
		}

        /// <summary>
        /// Instance method that returns an array of directory paths associated
        /// with the given IndexSetting.
        /// </summary>
        /// <param name="oIndexSettingRefresh">IndexSetting enumeration value</param>
        /// <returns>DirectoryInfo[] of directory paths</returns>
		public DirectoryInfo[] RefreshDirectories(IndexSetting oIndexSettingRefresh)
		{
			this._arRefreshDirectories=null;
			if (oIndexSettingRefresh==IndexSetting.IndexA)
				this._arRefreshDirectories = this._arIndexADirectories;
			else if (oIndexSettingRefresh==IndexSetting.IndexB)
				this._arRefreshDirectories = this._arIndexBDirectories;
			return this._arRefreshDirectories;
		}
	}
}
