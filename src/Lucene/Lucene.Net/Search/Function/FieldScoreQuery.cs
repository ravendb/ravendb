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

namespace Lucene.Net.Search.Function
{

    /// <summary> A query that scores each document as the value of the numeric input field.
    /// <p/> 
    /// The query matches all documents, and scores each document according to the numeric 
    /// value of that field. 
    /// <p/>
    /// It is assumed, and expected, that:
    /// <list type="bullet">
    /// <item>The field used here is indexed, and has exactly 
    /// one token in every scored document.</item> 
    /// <item>Best if this field is un_tokenized.</item>
    /// <item>That token is parsable to the selected type.</item>
    /// </list>
    /// <p/>  
    /// Combining this query in a FunctionQuery allows much freedom in affecting document scores.
    /// Note, that with this freedom comes responsibility: it is more than likely that the
    /// default Lucene scoring is superior in quality to scoring modified as explained here.
    /// However, in some cases, and certainly for research experiments, this capability may turn useful.
    /// <p/>
    /// When contructing this query, select the appropriate type. That type should match the data stored in the
    /// field. So in fact the "right" type should be selected before indexing. Type selection
    /// has effect on the RAM usage: 
    /// <list type="bullet">
    /// <item><see cref="Type.BYTE" /> consumes 1 * maxDocs bytes.</item>
    /// <item><see cref="Type.SHORT" /> consumes 2 * maxDocs bytes.</item>
    /// <item><see cref="Type.INT" /> consumes 4 * maxDocs bytes.</item>
    /// <item><see cref="Type.FLOAT" /> consumes 8 * maxDocs bytes.</item>
    /// </list>
    /// <p/>
    /// <b>Caching:</b>
    /// Values for the numeric field are loaded once and cached in memory for further use with the same IndexReader. 
    /// To take advantage of this, it is extremely important to reuse index-readers or index-searchers, 
    /// otherwise, for instance if for each query a new index reader is opened, large penalties would be 
    /// paid for loading the field values into memory over and over again!
    /// 
    /// <p/><font color="#FF0000">
    /// WARNING: The status of the <b>Search.Function</b> package is experimental. 
    /// The APIs introduced here might change in the future and will not be 
    /// supported anymore in such a case.</font>
    /// </summary>
    [Serializable]
    public class FieldScoreQuery:ValueSourceQuery
	{
		
		/// <summary> Type of score field, indicating how field values are interpreted/parsed.  
		/// <p/>
		/// The type selected at search search time should match the data stored in the field. 
		/// Different types have different RAM requirements: 
		/// <list type="bullet">
		/// <item><see cref="BYTE" /> consumes 1 * maxDocs bytes.</item>
		/// <item><see cref="SHORT" /> consumes 2 * maxDocs bytes.</item>
		/// <item><see cref="INT" /> consumes 4 * maxDocs bytes.</item>
		/// <item><see cref="FLOAT" /> consumes 8 * maxDocs bytes.</item>
		/// </list>
		/// </summary>
		public class Type
		{
			
			/// <summary>field values are interpreted as numeric byte values. </summary>
			public static readonly Type BYTE = new Type("byte");
			
			/// <summary>field values are interpreted as numeric short values. </summary>
			public static readonly Type SHORT = new Type("short");
			
			/// <summary>field values are interpreted as numeric int values. </summary>
			public static readonly Type INT = new Type("int");
			
			/// <summary>field values are interpreted as numeric float values. </summary>
			public static readonly Type FLOAT = new Type("float");
			
			private System.String typeName;
			internal Type(System.String name)
			{
				this.typeName = name;
			}
			/*(non-Javadoc) <see cref="java.lang.Object.toString() */
			public override System.String ToString()
			{
				return GetType().FullName + "::" + typeName;
			}
		}
		
		/// <summary> Create a FieldScoreQuery - a query that scores each document as the value of the numeric input field.
		/// <p/>
		/// The <c>type</c> param tells how to parse the field string values into a numeric score value.
		/// </summary>
		/// <param name="field">the numeric field to be used.
		/// </param>
		/// <param name="type">the type of the field: either
		/// <see cref="Type.BYTE" />, <see cref="Type.SHORT" />, <see cref="Type.INT" />, or <see cref="Type.FLOAT" />. 
		/// </param>
		public FieldScoreQuery(System.String field, Type type):base(GetValueSource(field, type))
		{
		}
		
		// create the appropriate (cached) field value source.  
		private static ValueSource GetValueSource(System.String field, Type type)
		{
			if (type == Type.BYTE)
			{
				return new ByteFieldSource(field);
			}
			if (type == Type.SHORT)
			{
				return new ShortFieldSource(field);
			}
			if (type == Type.INT)
			{
				return new IntFieldSource(field);
			}
			if (type == Type.FLOAT)
			{
				return new FloatFieldSource(field);
			}
			throw new System.ArgumentException(type + " is not a known Field Score Query Type!");
		}
	}
}