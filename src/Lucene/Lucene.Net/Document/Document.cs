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
using Lucene.Net.Store;

// for javadoc
using IndexReader = Lucene.Net.Index.IndexReader;
using ScoreDoc = Lucene.Net.Search.ScoreDoc;
using Searcher = Lucene.Net.Search.Searcher;

namespace Lucene.Net.Documents
{

    /// <summary>Documents are the unit of indexing and search.
    /// 
    /// A Document is a set of fields.  Each field has a name and a textual value.
    /// A field may be <see cref="IFieldable.IsStored()">stored</see> with the document, in which
    /// case it is returned with search hits on the document.  Thus each document
    /// should typically contain one or more stored fields which uniquely identify
    /// it.
    /// 
    /// <p/>Note that fields which are <i>not</i> <see cref="IFieldable.IsStored()">stored</see> are
    /// <i>not</i> available in documents retrieved from the index, e.g. with <see cref="ScoreDoc.Doc" />,
    /// <see cref="Searcher.Doc(int)" /> or <see cref="IndexReader.Document(int)" />.
    /// </summary>
    [Serializable]
    public sealed class Document
	{
		private class AnonymousClassEnumeration : System.Collections.IEnumerator
		{
			public AnonymousClassEnumeration(Document enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(Document enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
				iter = Enclosing_Instance.fields.GetEnumerator();
			}
			private System.Object tempAuxObj;
			public bool MoveNext()
			{
				bool result = HasMoreElements();
				if (result)
				{
					tempAuxObj = NextElement();
				}
				return result;
			}
			public void  Reset()
			{
				tempAuxObj = null;
			}
			public System.Object Current
			{
				get
				{
					return tempAuxObj;
				}
				
			}
			private Document enclosingInstance;
			public Document Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal System.Collections.IEnumerator iter;
			public bool HasMoreElements()
			{
				return iter.MoveNext();
			}
			public System.Object NextElement()
			{
				return iter.Current;
			}
		}
		internal System.Collections.Generic.IList<IFieldable> fields = new System.Collections.Generic.List<IFieldable>();
		private float boost = 1.0f;
		
		/// <summary>Constructs a new document with no fields. </summary>
		public Document()
		{
		}


	    /// <summary>Gets or sets, at indexing time, the boost factor. 
	    /// <para>
	    /// The default is 1.0
	    /// </para>
	    /// <p/>Note that once a document is indexed this value is no longer available
	    /// from the index.  At search time, for retrieved documents, this method always 
	    /// returns 1. This however does not mean that the boost value set at  indexing 
	    /// time was ignored - it was just combined with other indexing time factors and 
	    /// stored elsewhere, for better indexing and search performance. (For more 
	    /// information see the "norm(t,d)" part of the scoring formula in 
	    /// <see cref="Lucene.Net.Search.Similarity">Similarity</see>.)
	    /// </summary>
	    public float Boost
	    {
	        get { return boost; }
	        set { this.boost = value; }
	    }

	    /// <summary> <p/>Adds a field to a document.  Several fields may be added with
		/// the same name.  In this case, if the fields are indexed, their text is
		/// treated as though appended for the purposes of search.<p/>
		/// <p/> Note that add like the removeField(s) methods only makes sense 
		/// prior to adding a document to an index. These methods cannot
		/// be used to change the content of an existing index! In order to achieve this,
		/// a document has to be deleted from an index and a new changed version of that
		/// document has to be added.<p/>
		/// </summary>
		public void  Add(IFieldable field)
		{
			fields.Add(field);
		}
		
		/// <summary> <p/>Removes field with the specified name from the document.
		/// If multiple fields exist with this name, this method removes the first field that has been added.
		/// If there is no field with the specified name, the document remains unchanged.<p/>
		/// <p/> Note that the removeField(s) methods like the add method only make sense 
		/// prior to adding a document to an index. These methods cannot
		/// be used to change the content of an existing index! In order to achieve this,
		/// a document has to be deleted from an index and a new changed version of that
		/// document has to be added.<p/>
		/// </summary>
		public void  RemoveField(System.String name)
		{
			System.Collections.Generic.IEnumerator<IFieldable> it = fields.GetEnumerator();
			while (it.MoveNext())
			{
				IFieldable field = it.Current;
				if (field.Name.Equals(name))
				{
                    fields.Remove(field);
					return ;
				}
			}
		}
		
		/// <summary> <p/>Removes all fields with the given name from the document.
		/// If there is no field with the specified name, the document remains unchanged.<p/>
		/// <p/> Note that the removeField(s) methods like the add method only make sense 
		/// prior to adding a document to an index. These methods cannot
		/// be used to change the content of an existing index! In order to achieve this,
		/// a document has to be deleted from an index and a new changed version of that
		/// document has to be added.<p/>
		/// </summary>
		public void  RemoveFields(System.String name)
		{
            for (int i = fields.Count - 1; i >= 0; i--)
            {
                IFieldable field = fields[i];
                if (field.Name.Equals(name))
                {
                    fields.RemoveAt(i);
                }
            }
		}
		
		/// <summary>Returns a field with the given name if any exist in this document, or
		/// null.  If multiple fields exists with this name, this method returns the
		/// first value added.
		/// Do not use this method with lazy loaded fields.
		/// </summary>
		public Field GetField(System.String name)
		{
		    return (Field) GetFieldable(name);
		}
		
		
		/// <summary>Returns a field with the given name if any exist in this document, or
		/// null.  If multiple fields exists with this name, this method returns the
		/// first value added.
		/// </summary>
		public IFieldable GetFieldable(System.String name)
		{
			foreach(IFieldable field in fields)
            {
				if (field.Name.Equals(name))
					return field;
			}
			return null;
		}
		
		/// <summary>Returns the string value of the field with the given name if any exist in
		/// this document, or null.  If multiple fields exist with this name, this
		/// method returns the first value added. If only binary fields with this name
		/// exist, returns null.
		/// </summary>
		public System.String Get(System.String name, IState state)
		{
			foreach(IFieldable field in fields)
			{
				if (field.Name.Equals(name) && (!field.IsBinary))
					return field.StringValue(state);
			}
			return null;
		}
		
		/// <summary>Returns a List of all the fields in a document.
		/// <p/>Note that fields which are <i>not</i> <see cref="IFieldable.IsStored()">stored</see> are
		/// <i>not</i> available in documents retrieved from the
		/// index, e.g. <see cref="Searcher.Doc(int)" /> or <see cref="IndexReader.Document(int)" />.
		/// </summary>
		public System.Collections.Generic.IList<IFieldable> GetFields()
		{
			return fields;
		}
		
		private static readonly Field[] NO_FIELDS = new Field[0];
		
		/// <summary> Returns an array of <see cref="Field" />s with the given name.
		/// Do not use with lazy loaded fields.
		/// This method returns an empty array when there are no
		/// matching fields.  It never returns null.
		/// 
		/// </summary>
		/// <param name="name">the name of the field
		/// </param>
		/// <returns> a <c>Field[]</c> array
		/// </returns>
		public Field[] GetFields(System.String name)
		{
			var result = new System.Collections.Generic.List<Field>();
			foreach(IFieldable field in fields)
			{
				if (field.Name.Equals(name))
				{
					result.Add((Field)field);
				}
			}
			
			if (result.Count == 0)
				return NO_FIELDS;
			
			return result.ToArray();
		}
		
		
		private static readonly IFieldable[] NO_FIELDABLES = new IFieldable[0];
		
		/// <summary> Returns an array of <see cref="IFieldable" />s with the given name.
		/// This method returns an empty array when there are no
		/// matching fields.  It never returns null.
		/// 
		/// </summary>
		/// <param name="name">the name of the field
		/// </param>
		/// <returns> a <c>Fieldable[]</c> array
		/// </returns>
		public IFieldable[] GetFieldables(System.String name)
		{
			var result = new System.Collections.Generic.List<IFieldable>();
			foreach(IFieldable field in fields)
			{
				if (field.Name.Equals(name))
				{
					result.Add(field);
				}
			}
			
			if (result.Count == 0)
				return NO_FIELDABLES;
			
			return result.ToArray();
		}
		
		
		private static readonly System.String[] NO_STRINGS = new System.String[0];
		
		/// <summary> Returns an array of values of the field specified as the method parameter.
		/// This method returns an empty array when there are no
		/// matching fields.  It never returns null.
		/// </summary>
		/// <param name="name">the name of the field
		/// </param>
		/// <returns> a <c>String[]</c> of field values
		/// </returns>
		public System.String[] GetValues(System.String name, IState state)
		{
			var result = new System.Collections.Generic.List<string>();
			foreach(IFieldable field in fields)
			{
				if (field.Name.Equals(name) && (!field.IsBinary))
					result.Add(field.StringValue(state));
			}
			
			if (result.Count == 0)
				return NO_STRINGS;
			
			return result.ToArray();
		}
		
		private static readonly byte[][] NO_BYTES = new byte[0][];
		
		/// <summary> Returns an array of byte arrays for of the fields that have the name specified
		/// as the method parameter.  This method returns an empty
		/// array when there are no matching fields.  It never
		/// returns null.
		/// 
		/// </summary>
		/// <param name="name">the name of the field
		/// </param>
		/// <returns> a <c>byte[][]</c> of binary field values
		/// </returns>
		public byte[][] GetBinaryValues(System.String name, IState state)
		{
			var result = new System.Collections.Generic.List<byte[]>();
			foreach(IFieldable field in fields)
			{
				if (field.Name.Equals(name) && (field.IsBinary))
					result.Add(field.GetBinaryValue(state));
			}
			
			if (result.Count == 0)
				return NO_BYTES;

            return result.ToArray();
        }
		
		/// <summary> Returns an array of bytes for the first (or only) field that has the name
		/// specified as the method parameter. This method will return <c>null</c>
		/// if no binary fields with the specified name are available.
		/// There may be non-binary fields with the same name.
		/// 
		/// </summary>
		/// <param name="name">the name of the field.
		/// </param>
		/// <returns> a <c>byte[]</c> containing the binary field value or <c>null</c>
		/// </returns>
		public byte[] GetBinaryValue(System.String name, IState state)
		{
			foreach(IFieldable field in fields)
			{
				if (field.Name.Equals(name) && (field.IsBinary))
					return field.GetBinaryValue(state);
			}
			return null;
		}
		
		/// <summary>Prints the fields of a document for human consumption. </summary>
		public override System.String ToString()
		{
			System.Text.StringBuilder buffer = new System.Text.StringBuilder();
			buffer.Append("Document<");
			for (int i = 0; i < fields.Count; i++)
			{
				IFieldable field = fields[i];
				buffer.Append(field.ToString());
				if (i != fields.Count - 1)
					buffer.Append(" ");
			}
			buffer.Append(">");
			return buffer.ToString();
		}

        public System.Collections.Generic.IList<IFieldable> fields_ForNUnit
        {
            get { return fields; }
        }
	}
}