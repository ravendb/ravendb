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
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Distributed;

namespace Lucene.Net.Distributed.Indexing
{

	/// <summary>
	/// Base class representing a record to be added to a Lucene index.
    /// <para>
    /// IndexDocument contains a RecordId and a Lucene.Net.Document. The RecordId
    /// is interrogated to determine which index to add the associated 
    /// Lucene.Net.Document.
    /// </para>
	/// </summary>
	[Serializable]
	public abstract class IndexDocument
	{
		#region Variables
		protected Document _oDocument;
		protected int _intRecordId;
		public static BinaryFormatter Formatter = new BinaryFormatter();
        private static string filepath = (ConfigurationManager.AppSettings["IndexDocumentPath"] != null ? ConfigurationManager.AppSettings["IndexDocumentPath"] : "");
		private static string endwhack = (filepath.EndsWith(@"\") ? "" : @"\");
		private DateTime _eDateTime;
		#endregion

		#region Constructors
        /// <summary>
        /// Empty public constructor.
        /// </summary>
		public IndexDocument()
		{
		}

        /// <summary>
        /// Base constructor accepting only a RecordId. Useful for classes that 
        /// will have no associated Document, i.e. deletes.
        /// </summary>
        /// <param name="iRecordId">The source recordId (see also <seealso cref="#">IndexSet.IdColumn</seealso>) </param>
		public IndexDocument(int iRecordId)
		{
			this._intRecordId = iRecordId;
			this._oDocument = new Document();
			this._eDateTime = DateTime.Now;
		}

		public IndexDocument(Document oDocument, int iRecordId)
		{
			this._oDocument = oDocument;
			this._intRecordId = iRecordId;
			this._eDateTime = DateTime.Now;
		}

		#endregion

		#region Properties
		public Document Document
		{
			get {return this._oDocument;}
		}

		public int RecordId
		{
			get {return this._intRecordId;}
		}

        public virtual Analyzer GetAnalyzer()
        {
            return null;
        }

		public string FileName
		{
			get { return Environment.MachineName + "_" + this.GetType().ToString() + "_" + this.RecordId.ToString() + "_" + this.DateTime.Ticks.ToString() + ".bin"; }
		}
		private DateTime DateTime
		{
			get { return this._eDateTime; }
		}

		#endregion

		#region Methods
		public void Save()
		{
			try
			{
				FileStream fs = File.Open(filepath + endwhack + this.FileName, FileMode.Create, FileAccess.ReadWrite);
				IndexDocument.Formatter.Serialize(fs, this);
				fs.Close();
			}
			catch (SerializationException se)
			{
				throw (se);
			}
			catch (NullReferenceException nre)
			{
				throw (nre);
			}
		}
		public void Save(string filePath)
		{
			try
			{
				FileStream fs = File.Open(filePath + endwhack + this.FileName, FileMode.Create, FileAccess.ReadWrite);
				IndexDocument.Formatter.Serialize(fs, this);
				fs.Close();
			}
			catch (SerializationException se)
			{
				throw (se);
			}
			catch (NullReferenceException nre)
			{
				throw (nre);
			}
		}
		#endregion


	}
}
