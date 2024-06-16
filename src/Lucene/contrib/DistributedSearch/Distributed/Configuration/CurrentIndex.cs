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
using System.Collections.Specialized;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Xml;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Lucene.Net.Distributed;

namespace Lucene.Net.Distributed.Configuration
{
	public enum IndexSetting
	{
		NoSetting	= 0,
		IndexA		= 1,
		IndexB		= 2
	}

    /// <summary>
    /// Definition of current index information managed by the 
    /// LuceneUpdater windows service. The <copy> node within the
    /// <indexset> node represents the information needed to load
    /// a CurrentIndex object for a given IndexSet.
    /// 
    /// An example configuration would look like the following:
    /// <code>
    ///   <indexsets RAMAllocation="100000" CompoundFile="true" DeltaDirectory="c:\indexes\indexdocuments">
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
    public class CurrentIndex
	{
		#region Variables
		private static readonly string CURRENTINDEX = "currentIndex";
		private static readonly string INDEX_A = "indexA";
		private static readonly string INDEX_B = "indexB";
		private static readonly string TOGGLE = "toggle";
		private string _strLocalPath;
		private string _strStatusDir;
		private string _strIndexAPath;
		private string _strIndexBPath;
		private bool _bIndexChanged=false;

        private int _mergeFactor = (ConfigurationManager.AppSettings["IndexMergeFactor"] != null ? Convert.ToInt32(ConfigurationManager.AppSettings["IndexMergeFactor"]) : 5);
        private int _maxMergeDocs = (ConfigurationManager.AppSettings["IndexMaxMergeDocs"] != null ? Convert.ToInt32(ConfigurationManager.AppSettings["IndexMaxMergeDocs"]) : 9999999);

		#endregion

		#region Constructors
		/// <summary>
		/// Constructs a new CurrentIndex using the XmlNode value (from IndexSetConfigurationHandler configuration)
		/// </summary>
		/// <param name="node">XmlNode containing configuration information</param>
		/// <param name="strLocalPath">Local filesystem path to source index</param>
		public CurrentIndex(XmlNode node, string strLocalPath)
		{
			this._strLocalPath=strLocalPath;
			this.LoadValues(node);
		}

		/// <summary>
		/// Constructs a shell CurrentIndex. Use this constructor to interact 
		/// with the underlying status and toggle files ONLY.
		/// </summary>
		/// <param name="sStatusDir">Filesystem path to the status and toggle files for an index</param>
		public CurrentIndex(string sStatusDir)
		{
			this._strStatusDir=sStatusDir;
		}
		#endregion

		#region Internals
        /// <summary>
        /// Internal routine for use by constructor that accepts a configuration
        /// entry structured as XmlNode.
        /// </summary>
        /// <param name="node">XmlNode containing configuration information</param>
		internal void LoadValues(XmlNode node)
		{
			foreach (XmlNode c in node.ChildNodes)
			{
				if (c.Name.ToLower()=="targetpath")
				{
					this._strIndexAPath = c.Attributes["indexA"].Value;
					this._strIndexBPath = c.Attributes["indexB"].Value;
				}
				else if (c.Name.ToLower()=="statusdir")
				{
					this._strStatusDir = c.Attributes["value"].Value;
				}
			}
            this.CheckValidConfiguration(node);
		}
		#endregion

		#region Public properties
		/// <summary>
		/// Filesystem path to the local source for an index; this is the path to the master index.
		/// </summary>
		public string LocalPath
		{
			get {return this._strLocalPath;}
		}
		/// <summary>
		/// Filesystem path to a LuceneServer's status and toggle file for a given IndexSet
		/// </summary>
		public string StatusDirectory
		{
			get {return this._strStatusDir;}
		}

		/// <summary>
		/// Indicates the current index directory (IndexSetting enum) in use (the online set)
		/// </summary>
		public IndexSetting IndexSetting
		{
			get
			{
				string input=(this.GetCurrentIndex());
				return (input==CurrentIndex.INDEX_A ? IndexSetting.IndexA : (input==CurrentIndex.INDEX_B ? IndexSetting.IndexB : IndexSetting.IndexA));
			}
		}

		/// <summary>
		/// Indicates the index directory to be used in any index searcher refresh
		/// by determining if any updates have been applied
		/// </summary>
		public IndexSetting IndexSettingRefresh
		{
			get
			{
				if (this.HasChanged)
				{
					return (this.IndexSetting==IndexSetting.IndexA ? IndexSetting.IndexB : (this.IndexSetting==IndexSetting.IndexB ? IndexSetting.IndexA : IndexSetting.IndexB ));
				}
				else
				{
					return this.IndexSetting;
				}
			}
		}

		/// <summary>
		/// Indicates if the current index permits updated indexes to be copied to CopyTargetPath
		/// </summary>
		public bool CanCopy
		{
			get {return (!this.GetToggle() && (this.LocalIndexVersion!=this.TargetIndexVersion));}
		}

		/// <summary>
		/// Indicates if the current index has pending updates (in the offline directory) to be used by an index searcher
        /// in a refresh evaluation
		/// </summary>
		public bool HasChanged
		{
			get {return this.GetToggle();}
		}

		/// <summary>
		/// The target directory path to be used when updating the offline index
		/// </summary>
		public string CopyTargetPath
		{
			get {return (this.IndexSetting==IndexSetting.IndexA ? this._strIndexBPath : (this.IndexSetting==IndexSetting.IndexB ? this._strIndexAPath : ""));}
		}
		#endregion

		#region Public methods
		/// <summary>
		/// Method that executes a filesystem copy of all directory files from a local path to 
        /// the proper offline index.  This method ensures no conflicts occur with the online index.
		/// </summary>
		/// <returns>bool</returns>
		public bool Copy()
		{
			try
			{
				if (this.CanCopy && this.CopyTargetPath!="")
				{
					this.DeleteDirectoryFiles(this.CopyTargetPath);
					this.CopyDirectory(this._strLocalPath, this.CopyTargetPath);
					return true;
				}
				else
				{
					return false;
				}
			}
			catch (Exception e)
			{
                //Do something with e
				return false;
			}
		}

        /// <summary>
        /// Method that executes a filesystem copy of updated or new files from a local path to 
        /// the proper offline index.  This method ensures no conflicts occur with the online index.
        /// </summary>
        /// <returns></returns>
		public bool CopyIncremental()
		{
			try
			{
				if (this.CanCopy && this.CopyTargetPath!="")
				{
					this.CopyDirectoryIncremental(this._strLocalPath, this.CopyTargetPath);
					return true;
				}
				else
				{
					return false;
				}
			}
			catch (Exception e)
			{
                //Do something with e
                return false;
			}
		}

		/// <summary>
		/// Takes a name/value pair collection to be used in updating an index. 
		/// Deletes are necessary to ensure no duplication occurs within the index.
		/// </summary>
		/// <param name="nvcDeleteCollection">Set of record IDs (with underlying field name) to be applied for index updating</param>
		public void ProcessLocalIndexDeletes(NameValueCollection nvcDeleteCollection)
		{
			if (IndexReader.IndexExists(this._strLocalPath) && nvcDeleteCollection.Count>0)
			{
				IndexReader idxDeleter = IndexReader.Open(this._strLocalPath);
				string[] arKeys = nvcDeleteCollection.AllKeys;
				int xDelete=0;
				for (int k=0;k<arKeys.Length;k++)
				{
					string[] arKeyValues=nvcDeleteCollection.GetValues(arKeys[k]);
					for (int v=0;v<arKeyValues.Length;v++)
						xDelete=idxDeleter.DeleteDocuments(new Term(arKeys[k].ToString(),arKeyValues[v].ToString()));
				}
				idxDeleter.Close();
			}
		}

		/// <summary>
		/// Executes a loop on the Documents arraylist, adding each one to the index with the associated analyzer.
		/// </summary>
		/// <param name="oAnalyzer">Analyzer to be used in index document addition</param>
		/// <param name="alAddDocuments">Arraylist of Lucene Document objects to be inserted in the index</param>
		/// <param name="bCompoundFile">Setting to dictate if the index should use compound format</param>
		public void ProcessLocalIndexAdditions(Analyzer oAnalyzer, Hashtable htAddDocuments, bool bCompoundFile)
		{
			IndexWriter idxWriter = this.GetIndexWriter(this._strLocalPath, oAnalyzer, bCompoundFile);
            idxWriter.SetMergeFactor(5);
            idxWriter.SetMaxMergeDocs(9999999);

			foreach (DictionaryEntry de in htAddDocuments)
			{
				Document d = (Document)de.Key;
				Analyzer a = (Analyzer)de.Value;
				idxWriter.AddDocument(d,a);
			}
			idxWriter.Close();
		}

		/// <summary>
		/// Single method to be used by a searchhost to indicate an index refresh has completed.
		/// </summary>
		public void IndexRefresh()
		{
			if (this.HasChanged)
			{
				this.SetCurrentIndex(this.IndexSettingRefresh);
				this.SetToggle(false);
			}
		}

		/// <summary>
		/// Single method to be used by an index updater to indicate an index update has completed.
		/// </summary>
		public void UpdateRefresh()
		{
			this.SetToggle(true);
		}
		#endregion

		#region Private properties
		/// <summary>
		/// The filesystem path to the underlying index status file
		/// </summary>
		private string CurrentIndexFile
		{
			get {return (this._strStatusDir+(this._strStatusDir.EndsWith(@"\") ? "" : @"\")+CURRENTINDEX);}
		}
		/// <summary>
		/// The filesystem path to the underlying index toggle file
		/// </summary>
		private string ToggleFile
		{
			get {return (this._strStatusDir+(this._strStatusDir.EndsWith(@"\") ? "" : @"\")+TOGGLE);}
		}

		#endregion

		#region Private methods

        /// <summary>
        /// Validation routine to ensure all required values were present within xml configuration node
        /// used in constructor.
        /// </summary>
        /// <param name="node">XmlNode containing configuration information</param>
        private void CheckValidConfiguration(XmlNode node)
        {
            if (this._strLocalPath == null) throw new ConfigurationErrorsException("CurrentIndex local path invalid: "+Environment.NewLine+node.OuterXml);
            if (this._strStatusDir == null) throw new ConfigurationErrorsException("CurrentIndex statusDir invalid: " + Environment.NewLine + node.OuterXml);
            if (this._strIndexAPath == null) throw new ConfigurationErrorsException("CurrentIndex indexA invalid: " + Environment.NewLine + node.OuterXml);
            if (this._strIndexBPath == null) throw new ConfigurationErrorsException("CurrentIndex indexB invalid: " + Environment.NewLine + node.OuterXml);
        }

		/// <summary>
		/// Returns the current toggle file setting
		/// </summary>
		/// <returns>bool</returns>
		private bool GetToggle()
		{
			bool bValue=false;
			string input="";
			try
			{
				if (!File.Exists(this.ToggleFile))
				{
					this.SetToggle(false);
				}
				else
				{
					StreamReader sr = File.OpenText(this.ToggleFile);
					input = sr.ReadLine();
					sr.Close();
					bValue = (input.ToLower()=="true" ? true : false);
				}
			}
			catch (Exception ex)
			{
				//Do something with ex
			}
			return bValue;
		}

		/// <summary>
		/// Returns the current status file setting
		/// </summary>
		/// <returns>string</returns>
		private string GetCurrentIndex()
		{
			string input="";
			try
			{
				if (!File.Exists(this.CurrentIndexFile))
				{
					this.SetCurrentIndex(IndexSetting.IndexA);
					input=IndexSetting.IndexA.ToString();
				}
				else
				{
					StreamReader sr = File.OpenText(this.CurrentIndexFile);
					input = sr.ReadLine();
					sr.Close();
				}
			}
			catch (Exception ex)
			{
				//Do something with ex
			}
			return input;
		}

		/// <summary>
		/// Updates the status file with the IndexSetting value parameter
		/// </summary>
		/// <param name="eIndexSetting">Setting to be applied to the status file</param>
		private void SetCurrentIndex(IndexSetting eIndexSetting)
		{
			try
			{
				StreamWriter sw = File.CreateText(this.CurrentIndexFile);
				sw.WriteLine((eIndexSetting==IndexSetting.IndexA ? CurrentIndex.INDEX_A : CurrentIndex.INDEX_B));
				sw.Close();
			}
			catch (Exception ex)
			{
				//Do something with ex
			}
		}

		/// <summary>
		/// IndexWriter that can be used to apply updates to an index
		/// </summary>
		/// <param name="indexPath">File system path to the target index</param>
		/// <param name="oAnalyzer">Lucene Analyzer to be used by the underlying IndexWriter</param>
		/// <param name="bCompoundFile">Setting to dictate if the index should use compound format</param>
		/// <returns></returns>
		private IndexWriter GetIndexWriter(string indexPath, Analyzer oAnalyzer, bool bCompoundFile)
		{
			bool bExists = System.IO.Directory.Exists(indexPath);
			if (bExists==false)
				System.IO.Directory.CreateDirectory(indexPath);
			bExists=IndexReader.IndexExists(FSDirectory.GetDirectory(indexPath, false));
			IndexWriter idxWriter = new IndexWriter(indexPath, oAnalyzer, !bExists);
			idxWriter.SetUseCompoundFile(bCompoundFile);
			return idxWriter;
		}

		/// <summary>
		/// Updates the toggle file with the bool value parameter
		/// </summary>
		/// <param name="bValue">Bool to be applied to the toggle file</param>
		private void SetToggle(bool bValue)
		{
			try
			{
				StreamWriter sw = File.CreateText(this.ToggleFile);
				sw.WriteLine(bValue.ToString());
				sw.Close();
				this._bIndexChanged=bValue;
			}
			catch (Exception ex)
			{
				//Do something with ex
			}
		}

		/// <summary>
		/// Returns the numeric index version (using Lucene objects) for the index located at LocalPath
		/// </summary>
		private long LocalIndexVersion
		{
			get {return IndexReader.GetCurrentVersion(this.LocalPath);}
		}
		/// <summary>
		/// Returns the numeric index version (using Lucene objects) for the index located at CopyTargetPath
		/// </summary>
		private long TargetIndexVersion
		{
			get {return (IndexReader.IndexExists(this.CopyTargetPath) ? IndexReader.GetCurrentVersion(this.CopyTargetPath) : 0);}
		}

		/// <summary>
		/// Deletes index files at the filesystem directoryPath location
		/// </summary>
		/// <param name="directoryPath">Filesystem path</param>
		private void DeleteDirectoryFiles(string directoryPath)
		{
			try
			{
				if(!System.IO.Directory.Exists(directoryPath))
					return;
				DirectoryInfo di = new DirectoryInfo(directoryPath);
				FileInfo[] arFi = di.GetFiles();
				foreach(FileInfo fi in arFi)
					fi.Delete();
			}
			catch(Exception e)
			{
				//Do something with e
			}
		}

		/// <summary>
		/// Copy all index files from the sourceDirPath to the destDirPath
		/// </summary>
		/// <param name="sourceDirPath">Filesystem path</param>
		/// <param name="destDirPath">Filesystem path</param>
		private void CopyDirectory(string sourceDirPath, string destDirPath)
		{
			string[] Files;

			if(destDirPath[destDirPath.Length-1]!=Path.DirectorySeparatorChar) 
				destDirPath+=Path.DirectorySeparatorChar;
			if(!System.IO.Directory.Exists(destDirPath)) System.IO.Directory.CreateDirectory(destDirPath);
			Files=System.IO.Directory.GetFileSystemEntries(sourceDirPath);
			foreach(string Element in Files)
			{
				// Sub directories
				if(System.IO.Directory.Exists(Element)) 
					CopyDirectory(Element,destDirPath+Path.GetFileName(Element));
					// Files in directory
				else 
					File.Copy(Element,destDirPath+Path.GetFileName(Element),true);
			}

		}

        /// <summary>
        /// Copy only new and updated index files from the sourceDirPath to the destDirPath
        /// </summary>
        /// <param name="sourceDirPath">Filesystem path</param>
        /// <param name="destDirPath">Filesystem path</param>
		private void CopyDirectoryIncremental(string sourceDirPath, string destDirPath)
		{
			string[] Files;

			if(destDirPath[destDirPath.Length-1]!=Path.DirectorySeparatorChar) 
				destDirPath+=Path.DirectorySeparatorChar;
			Files=System.IO.Directory.GetFileSystemEntries(sourceDirPath);
			if(!System.IO.Directory.Exists(destDirPath))
			{
				System.IO.Directory.CreateDirectory(destDirPath);
				foreach(string Element in Files)
				{
					// Sub directories
					if(System.IO.Directory.Exists(Element)) 
						CopyDirectory(Element,destDirPath+Path.GetFileName(Element));
						// Files in directory
					else 
						File.Copy(Element,destDirPath+Path.GetFileName(Element),true);
				}
			}
			else
			{
				foreach(string Element in Files)
				{
					if(System.IO.Directory.Exists(Element))
					{
						CopyDirectoryIncremental(Element,destDirPath+Path.GetFileName(Element));
					}
					else
					{
						if (System.IO.File.Exists(destDirPath+Path.GetFileName(Element)))
							this.CopyFileIncremental(Element, destDirPath+Path.GetFileName(Element));
						else
							File.Copy(Element,destDirPath+Path.GetFileName(Element),true);
					}
				}
			}
		}

        /// <summary>
        /// Evaluates the LastWriteTime and Length properties of two files to determine
        /// if a file should be copied.
        /// </summary>
        /// <param name="filepath1">Filesystem path</param>
        /// <param name="filepath2">Filesystem path</param>
        private void CopyFileIncremental(string filepath1, string filepath2)
		{
			FileInfo fi1 = new FileInfo(filepath1);
			FileInfo fi2 = new FileInfo(filepath2);
			if ((fi1.LastWriteTime!=fi2.LastWriteTime)||(fi1.Length!=fi2.Length))
				File.Copy(filepath1,filepath2,true);
		}
		#endregion

        #region Static methods
        /// <summary>
        /// Returns an Analyzer for the given AnalyzerType
        /// </summary>
        /// <param name="oAnalyzerType">Enumeration value</param>
        /// <returns>Analyzer</returns>
        public static Analyzer GetAnalyzer(AnalyzerType oAnalyzerType)
        {
            Analyzer oAnalyzer = null;
            switch (oAnalyzerType)
            {
                case AnalyzerType.SimpleAnalyzer:
                    oAnalyzer = new SimpleAnalyzer();
                    break;
                case AnalyzerType.StopAnalyzer:
                    oAnalyzer = new StopAnalyzer();
                    break;
                case AnalyzerType.WhitespaceAnalyzer:
                    oAnalyzer = new WhitespaceAnalyzer();
                    break;
                default:
                case AnalyzerType.StandardAnalyzer:
                    oAnalyzer = new StandardAnalyzer();
                    break;
            }
            return oAnalyzer;
        }
        #endregion

    }
}
