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
using System.IO;
using System.Xml;
using Lucene.Net.Distributed;
using Lucene.Net.Distributed.Configuration;

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
    public class IndexSets
	{
		private bool _bCompoundFile;
		private string _strDeltaDirectory;
		private IndexSet[] _arIndexSet;

        /// <summary>
        /// Accessor method for the configurable master indexes.
        /// </summary>
        public static IndexSets GetConfig
		{
            get { return (IndexSets)ConfigurationManager.GetSection("IndexSets"); }
		}

        /// <summary>
        /// Strongly-typed array of IndexSet objects as defined in a configuration section.
        /// </summary>
        /// <param name="node">XmlNode definition for a given IndexSet</param>
        public void LoadIndexSetArray(XmlNode node)
		{
			XmlAttributeCollection attributeCollection = node.Attributes;

            try
            {
                this._bCompoundFile = Convert.ToBoolean(attributeCollection["CompoundFile"].Value);
            }
            catch (Exception)
            {
                throw new ConfigurationErrorsException("CompoundFile invalid: " + Environment.NewLine + node.OuterXml);
            }

            try
            {
                this._strDeltaDirectory = attributeCollection["DeltaDirectory"].Value;
            }
            catch (Exception)
            {
                throw new ConfigurationErrorsException("DeltaDirectory invalid: " + Environment.NewLine + node.OuterXml);
            }

            if (node.ChildNodes.Count == 0)
                throw new ConfigurationErrorsException("No indexset definitions found " + Environment.NewLine + node.OuterXml);
            this._arIndexSet = new IndexSet[node.ChildNodes.Count];
            
			int x=0;
			foreach (XmlNode c in node.ChildNodes)
			{
				if (c.Name.ToLower()=="IndexSet")
				{
					IndexSet idxSet = new IndexSet(c);
					this._arIndexSet[x] = idxSet;
					x++;
				}

			}
		}

        /// <summary>
        /// Public constructor for IndexSets. An IndexSet is defined in XML configuration 
        /// and is loaded via a custom configuration handler.
        /// </summary>
        public IndexSets()
		{
		}

        /// <summary>
        /// Indicates if the indexes under configuration should be built in the Compound format.
        /// </summary>
		public bool CompoundFile
		{
			get {return this._bCompoundFile;}
		}

        /// <summary>
        /// Filesystem location of where pending update IndexDocuments are retrieved.
        /// </summary>
		public string DeltaDirectory
		{
			get {return this._strDeltaDirectory;}
		}

        /// <summary>
        /// Strongly-typed array of IndexSet objects as defined in a configuration section.
        /// </summary>
        public IndexSet[] IndexSetArray
		{
			get {return this._arIndexSet;}
		}

        /// <summary>
        /// Returns an IndexSet object for a given IndexDocument id value
        /// </summary>
        /// <param name="deleteId">Id value of the IndexDocument</param>
        /// <returns>The IndexSet containing the referenced IndexDocument</returns>
		public IndexSet GetIndexSet(int deleteId)
		{
			IndexSet getSet=null;
			foreach(IndexSet idxSet in this._arIndexSet)
			{
				if ((deleteId>=idxSet.BottomId)&&(deleteId<=idxSet.TopId))
					getSet=idxSet;
			}
			return getSet;
		}

        /// <summary>
        /// Queries the DeltaDirectory to access any IndexDocument files.  All IndexDocuments
        /// stored in the DeltaDirectory have been serialized to a file; the files are deserialized,
        /// evaluated for their id value (idcolumn) and added to the pending additions for the associated
        /// IndexSet.
        /// </summary>
        /// <param name="sourceDir">Filesystem path to the DeltaDirectory</param>
		public void LoadIndexDocuments(string sourceDir)
		{
			DirectoryInfo oDirectoryInfo = new DirectoryInfo(sourceDir);
			FileInfo[] arFiles = oDirectoryInfo.GetFiles("*.bin");
			Array.Sort(arFiles, new FileNameComparer());
			IndexSet idxSet;

			foreach (FileInfo fi in arFiles)
			{
				FileStream fs = new FileStream(fi.FullName, FileMode.Open);
				IndexDocument iDoc = (IndexDocument)IndexDocument.Formatter.Deserialize(fs);

				idxSet = this.GetIndexSet(iDoc.RecordId);
				if (idxSet != null)
				{
					idxSet.FileSystemDocuments.Add(fi.FullName);
					if (idxSet.IndexDocuments.ContainsKey(iDoc.RecordId))
					{
						IndexDocument curDoc = (IndexDocument)idxSet.IndexDocuments[iDoc.RecordId];
						idxSet.IndexDocuments.Add(iDoc.RecordId, iDoc);
					}
					else
						idxSet.IndexDocuments.Add(iDoc.RecordId, iDoc);
				}
				else
				{
					//Handling exceptions -- write file out somewhere else?
					if (ConfigurationManager.AppSettings["ExceptionsBasePath"] != null)
						iDoc.Save(ConfigurationManager.AppSettings["ExceptionsBasePath"]);
				}
				fs.Close();
			}
			oDirectoryInfo=null;
			arFiles=null;
		}

        /// <summary>
        /// Method to apply pending updates (additions & deletions) for all configured IndexSet objects.
        /// </summary>
		public void ProcessIndexDocuments()
		{
			foreach(IndexSet idxSet in this._arIndexSet)
			{
				if (idxSet.IndexDocuments.Count>0)
				{
					idxSet.CurrentIndex.ProcessLocalIndexDeletes(idxSet.GetDeletionCollection());
					idxSet.CurrentIndex.ProcessLocalIndexAdditions(idxSet.Analzyer, idxSet.Documents, this.CompoundFile);
				}
			}
		}

        /// <summary>
        /// Method to apply updated index files from master index to slave indexes
        /// </summary>
		public void CopyUpdatedFiles()
		{
			Hashtable htUpdates = new Hashtable();
			bool bCopy=false;
            foreach (IndexSet idxSet in this._arIndexSet)
			{
				bCopy=false;
				if (idxSet.CurrentIndex!=null && idxSet.CurrentIndex.CanCopy)
					bCopy=idxSet.CurrentIndex.CopyIncremental();
				if (bCopy && !htUpdates.ContainsKey(idxSet.CurrentIndex.StatusDirectory))
					htUpdates.Add(idxSet.CurrentIndex.StatusDirectory, idxSet.CurrentIndex);
			}

			foreach(DictionaryEntry de in htUpdates)
			{
				string sTargetDir = de.Key.ToString();
				CurrentIndex ci = (CurrentIndex)de.Value;
				ci.UpdateRefresh();
			}
		}

        /// <summary>
        /// Method to apply updated index files from master index to slave indexes
        /// </summary>
        /// <returns>Hashtable of updated indexes</returns>
		public Hashtable CopyAllFiles()
		{
			Hashtable htUpdates = new Hashtable();
			bool bCopy = false;
            foreach (IndexSet idxSet in this._arIndexSet)
			{
				bCopy = false;
				if (idxSet.CurrentIndex != null && idxSet.CurrentIndex.CanCopy)
					bCopy = idxSet.CurrentIndex.Copy();
				if (bCopy && !htUpdates.ContainsKey(idxSet.CurrentIndex.StatusDirectory))
					htUpdates.Add(idxSet.CurrentIndex.StatusDirectory, idxSet.CurrentIndex);
			}

			foreach (DictionaryEntry de in htUpdates)
			{
				string sTargetDir = de.Key.ToString();
				CurrentIndex ci = (CurrentIndex)de.Value;
				ci.UpdateRefresh();
			}

			return htUpdates;
		}

        /// <summary>
        /// Method to execute an index optimization for each configured IndexSet object
        /// </summary>
		public void OptimizeIndexes()
        {
            foreach (IndexSet idxSet in this._arIndexSet)
                idxSet.Optimize();
        }

        /// <summary>
        /// Method to finalize update process for each IndexSet object
        /// </summary>
        public void CompleteUpdate()
		{
            foreach (IndexSet idxSet in this._arIndexSet)
			{
				if (idxSet.FileSystemDocuments.Count>0)
				{
					foreach(string s in idxSet.FileSystemDocuments)
					{
						FileInfo fi = new FileInfo(s);
						fi.Delete();
					}
					idxSet.Reset();
				}
			}
		}

	}
}
