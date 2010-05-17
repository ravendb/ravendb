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

using HTMLParser = Lucene.Net.Demo.Html.HTMLParser;
using Lucene.Net.Documents;

namespace Lucene.Net.Demo
{
	
	/// <summary>A utility for making Lucene Documents for HTML documents. </summary>
	
	public class HTMLDocument
	{
		internal static char dirSep = System.IO.Path.DirectorySeparatorChar.ToString()[0];
		
		public static System.String Uid(System.IO.FileInfo f)
		{
			// Append path and date into a string in such a way that lexicographic
			// sorting gives the same results as a walk of the file hierarchy.  Thus
			// null (\u0000) is used both to separate directory components and to
			// separate the path from the date.
			return f.FullName.Replace(dirSep, '\u0000') + "\u0000" + DateTools.TimeToString(f.LastWriteTime.Millisecond, DateTools.Resolution.SECOND);
		}
		
		public static System.String Uid2url(System.String uid)
		{
			System.String url = uid.Replace('\u0000', '/'); // replace nulls with slashes
			return url.Substring(0, (url.LastIndexOf('/')) - (0)); // remove date from end
		}
		
		public static Document Document(System.IO.FileInfo f)
		{
			// make a new, empty document
			Document doc = new Document();
			
			// Add the url as a field named "path".  Use a field that is 
			// indexed (i.e. searchable), but don't tokenize the field into words.
			doc.Add(new Field("path", f.FullName.Replace(dirSep, '/'), Field.Store.YES, Field.Index.NOT_ANALYZED));
			
			// Add the last modified date of the file a field named "modified".  
			// Use a field that is indexed (i.e. searchable), but don't tokenize
			// the field into words.
			doc.Add(new Field("modified", DateTools.TimeToString(f.LastWriteTime.Millisecond, DateTools.Resolution.MINUTE), Field.Store.YES, Field.Index.NOT_ANALYZED));
			
			// Add the uid as a field, so that index can be incrementally maintained.
			// This field is not stored with document, it is indexed, but it is not
			// tokenized prior to indexing.
			doc.Add(new Field("uid", Uid(f), Field.Store.NO, Field.Index.NOT_ANALYZED));
			
			System.IO.FileStream fis = new System.IO.FileStream(f.FullName, System.IO.FileMode.Open, System.IO.FileAccess.Read);
			HTMLParser parser = new HTMLParser(fis);
			
			// Add the tag-stripped contents as a Reader-valued Text field so it will
			// get tokenized and indexed.
			doc.Add(new Field("contents", parser.GetReader()));
			
			// Add the summary as a field that is stored and returned with
			// hit documents for display.
			doc.Add(new Field("summary", parser.GetSummary(), Field.Store.YES, Field.Index.NO));
			
			// Add the title as a field that it can be searched and that is stored.
			doc.Add(new Field("title", parser.GetTitle(), Field.Store.YES, Field.Index.ANALYZED));
			
			// return the document
			return doc;
		}
		
		private HTMLDocument()
		{
		}
	}
}