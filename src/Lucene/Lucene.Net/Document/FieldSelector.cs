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

namespace Lucene.Net.Documents
{
    /// <summary> Similar to a <a href="http://download.oracle.com/javase/1.5.0/docs/api/java/io/FileFilter.html">
    /// java.io.FileFilter</a>, the FieldSelector allows one to make decisions about
	/// what Fields get loaded on a <see cref="Document" /> by <see cref="Lucene.Net.Index.IndexReader.Document(int,Lucene.Net.Documents.FieldSelector)" />
	/// </summary>
	public interface FieldSelector
	{
		
		/// <summary> </summary>
		/// <param name="fieldName">the field to accept or reject
		/// </param>
		/// <returns> an instance of <see cref="FieldSelectorResult" />
		/// if the <see cref="Field" /> named <c>fieldName</c> should be loaded.
		/// </returns>
		FieldSelectorResult Accept(System.String fieldName);
	}
}