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
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Xml;
using Lucene.Net.Distributed;
//using Lucene.Net.Distributed.Search;
using Lucene.Net.Distributed.Configuration;
using Documents = Lucene.Net.Documents;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;

namespace Lucene.Net.Distributed.Indexing
{
    /// <summary>
    /// Definition of configurable search indexes managed by the 
    /// LuceneUpdater windows service.
    /// 
    /// An example configuration would look like the following:
    /// <code>
    ///   <indexsets CompoundFile="true" DeltaDirectory="c:\indexes\indexdocuments">
    ///     <indexSet id="1" action="1" analyzer="1">
    ///         <add key="localpath" value="c:\lucene\masterindex\index1" />
    ///         <copy>
    ///             <targetPath indexA="\\LuceneServer\lucene\indexA\index1" indexB="\\LuceneServer\lucene\indexA\index1" />
    ///             <statusDir value="\\LuceneServer\lucene\statusfile\" />
    ///         </copy>
    ///         <add key="bottomid" value="1"/>
    ///         <add key="topid" value="1000"/>
    ///         <add key="idcolumn" value="pkId"/>
    ///     </indexSet>
    ///   </indexsets>
    /// </code>
    /// </summary>
    public class IndexSet
	{
		#region Variables
		private int _intId = -1;
		private string _strLocalPath;
		private string _strIdColumn;
		private int _intBottomId;
		private int _intTopId;
		private CurrentIndex _oCurrentIndex;
		private IndexAction _eIndexAction=IndexAction.NoAction;
		private AnalyzerType _eAnalyzerType=AnalyzerType.StandardAnalyzer;
		private Hashtable _htDocuments = new Hashtable();
		private Hashtable _htIndexDocuments = new Hashtable();
		private List<string> _alFileSystemDocuments = new List<string>();
		#endregion

		#region Constructors
        /// <summary>
        /// Public constructor for IndexSet. An IndexSet is defined in XML configuration 
        /// and is loaded via a custom configuration handler.
        /// </summary>
        /// <param name="node">XmlNode definition for a given IndexSet</param>
        public IndexSet(XmlNode node)
		{
			this.LoadValues(node);
		}

		#endregion

		#region Internal voids
        /// <summary>
        /// Internal load method called from the constructor. Loads underlying values
        /// based on Xml configuration.
        /// </summary>
        /// <param name="node">XmlNode definition for a given IndexSet</param>
		internal void LoadValues(XmlNode node)
		{
			XmlAttributeCollection attributeCollection = node.Attributes;
            try
            {
                this._intId = Convert.ToInt32(attributeCollection["id"].Value);
            }
            catch (Exception)
            {
                throw new ConfigurationErrorsException("IndexSet id invalid: " + Environment.NewLine + node.OuterXml);
            }

            try
            {
                this._eIndexAction = (IndexAction)Enum.Parse(typeof(IndexAction), attributeCollection["action"].Value);
            }
            catch (Exception)
            {
                throw new ConfigurationErrorsException("IndexSet "+this._intId.ToString()+" IndexAction invalid: " + Environment.NewLine + node.OuterXml);
            }

            try
            {
                if (attributeCollection["analyzer"] != null)
                    this._eAnalyzerType = (AnalyzerType)Enum.Parse(typeof(AnalyzerType), attributeCollection["analyzer"].Value);
            }
            catch (Exception)
            {
                throw new ConfigurationErrorsException("IndexSet " + this._intId.ToString() + " analyzer invalid: " + Environment.NewLine + node.OuterXml);
            }

            if (node.ChildNodes.Count==0)
                throw new ConfigurationErrorsException("IndexSet " + this._intId.ToString() + " configuration missing " + Environment.NewLine + node.OuterXml);

			foreach (XmlNode c in node.ChildNodes)
			{
				if (!c.HasChildNodes)
				{
					switch (c.Attributes["key"].Value.ToLower())
					{
						case "localpath":
							this._strLocalPath = c.Attributes["value"].Value;
							break;
						case "idcolumn":
							this._strIdColumn = c.Attributes["value"].Value;
							break;
						case "bottomid":
                            try
                            {
                                this._intBottomId = Convert.ToInt32(c.Attributes["value"].Value);
                            }
                            catch (Exception)
                            {
                                throw new ConfigurationErrorsException("IndexSet " + this._intId.ToString() + " bottomid invalid: " + Environment.NewLine + node.OuterXml);
                            }
							break;
						case "topid":
                            try
                            {
                                this._intTopId = Convert.ToInt32(c.Attributes["value"].Value);
                            }
                            catch (Exception)
                            {
                                throw new ConfigurationErrorsException("IndexSet " + this._intId.ToString() + " topid invalid: " + Environment.NewLine + node.OuterXml);
                            }
                            break;
					}
				}
				else
				{
					switch(c.Name.ToLower())
					{
						case "copy":
							if (this._strLocalPath!=null)
								LoadCopy(c,this._strLocalPath);
							else
								LoadCopy(c,node);
							break;
					}
				}
			}
            this.CheckValidSet(node);

		}

		internal void LoadCopy(XmlNode node, string localpath)
		{
			this._oCurrentIndex = new CurrentIndex(node,localpath);
		}

		internal void LoadCopy(XmlNode node, XmlNode masternode)
		{
			foreach (XmlNode c in node.ChildNodes)
			{
                if (c.Attributes["key"] != null)
                {
                    switch (c.Attributes["key"].Value.ToLower())
                    {
                        case "localpath":
                            this.LoadCopy(node, c.Attributes["value"].Value);
                            break;
                    }
                }
			}
		}

        private void CheckValidSet(XmlNode node)
        {
            if (this._strLocalPath==null) throw new ConfigurationErrorsException("IndexSet " + this._intId.ToString() + " LocalPath invalid: " + Environment.NewLine + node.OuterXml);
            if (this._eIndexAction==IndexAction.NoAction) throw new ConfigurationErrorsException("IndexSet " + this._intId.ToString() + " IndexAction undefined: " + Environment.NewLine + node.OuterXml);
            if (this._strIdColumn==null) throw new ConfigurationErrorsException("IndexSet " + this._intId.ToString() + " IdColumn undefined: " + Environment.NewLine + node.OuterXml);
        }

		#endregion

		#region Properties
        /// <summary>
        /// Unique identifier for an IndexSet within a configuration of multiple IndexSet objects
        /// </summary>
		public int Id
		{
			get {return this._intId;}
		}

        /// <summary>
        /// Enumeration dictating the type of updates to be applied to the underlying master index
        /// </summary>
		public IndexAction IndexAction
		{
			get {return this._eIndexAction;}
		}

        /// <summary>
        /// Enumeration dictating the type of Analyzer to be applied to IndexDocuments in update scenarios
        /// </summary>
		public AnalyzerType AnalyzerType
		{
			get {return this._eAnalyzerType;}
		}

        /// <summary>
        /// The Analyzer object used in application of IndexDocument updates 
        /// </summary>
		public Analyzer Analzyer
		{
			get {return CurrentIndex.GetAnalyzer(this._eAnalyzerType);}
		}

        /// <summary>
        /// Filesystem path to the master index
        /// </summary>
		public string LocalPath
		{
			get {return this._strLocalPath;}
		}

        /// <summary>
        /// String name representing the unique key for the given record in the index
        /// </summary>
		public string IdColumn
		{
			get {return this._strIdColumn;}
		}

        /// <summary>
        /// Minimum IdColumn value for a record in this index
        /// </summary>
		public int BottomId
		{
			get {return this._intBottomId;}
		}

        /// <summary>
        /// Maximum IdColumn value for a record in this index
        /// </summary>
		public int TopId
		{
			get {return this._intTopId;}
		}

        /// <summary>
        /// CurrentIndex object associated with this IndexSet.  The CurrentIndex is used
        /// in determining index settings and maintenance as well as managing physical file updates
        /// for index updates.
        /// </summary>
		public CurrentIndex CurrentIndex
		{
			get {return this._oCurrentIndex;}
		}

        /// <summary>
        /// List of filesystem paths representing files for the master index
        /// </summary>
        public List<string> FileSystemDocuments
        {
            get { return this._alFileSystemDocuments; }
        }

        /// <summary>
        /// Pending updates to be applied to the master index
        /// </summary>
        public Hashtable IndexDocuments
        {
            get { return this._htIndexDocuments; }
        }

        /// <summary>
        /// Retrieves the DeleteIndexDocuments from IndexDocuments
        /// </summary>
        public Hashtable Documents
        {
            get
            {
                this._htDocuments.Clear();
                foreach (DictionaryEntry de in this._htIndexDocuments)
                {
                    IndexDocument iDoc = (IndexDocument)de.Value;
                    if (!(iDoc is DeleteIndexDocument))
                        this._htDocuments.Add(iDoc.Document, iDoc.GetAnalyzer());
                }
                return this._htDocuments;
            }
        }
        #endregion

        #region Methods
        /// <summary>
        /// Retrieves a NameValueCollection of records to first be deleted from an index. Name/Value
        /// pair combination consists of IdColumn and RecordId from each IndexDocument in IndexDocuments.
        /// </summary>
        /// <returns></returns>
        public NameValueCollection GetDeletionCollection()
		{
			NameValueCollection nvc = new NameValueCollection(this._htDocuments.Count);
			foreach(DictionaryEntry de in this._htIndexDocuments)
				nvc.Add(this.IdColumn, ((IndexDocument)de.Value).RecordId.ToString());
			return nvc;
		}

        /// <summary>
        /// Clears the contents of Documents and IndexDocuments
        /// </summary>
		public void Reset()
		{
			this._htIndexDocuments.Clear();
			this._htDocuments.Clear();
		}

        /// <summary>
        /// Executes a Lucene.Net optimization against the referenced index.
        /// </summary>
        public void Optimize()
        {
            if (IndexReader.IndexExists(this._strLocalPath))
            {
                IndexWriter idxWriter = new IndexWriter(this._strLocalPath, this.Analzyer, false);
                idxWriter.SetMergeFactor(2);
                idxWriter.Optimize();
                idxWriter.Close();
            }
        }

        /// <summary>
        /// Indicates if a given recordId exists within the configuration
        /// definition of TopId and BottomId.
        /// </summary>
        /// <param name="recordId"></param>
        /// <returns></returns>
        public bool ContainsId(int recordId)
        {
            return (this._intTopId >= recordId && this._intBottomId <= recordId);
        }
        #endregion
    }
}
