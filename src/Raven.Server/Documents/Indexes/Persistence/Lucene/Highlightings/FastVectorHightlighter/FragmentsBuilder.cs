/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Lucene.Net.Index;
using Lucene.Net.Store;

namespace Lucene.Net.Search.Vectorhighlight
{
    /// <summary>
    /// FragmentsBuilder is an interface for fragments (snippets) builder classes.
    /// A FragmentsBuilder class can be plugged in to Highlighter.
    /// </summary>
    public interface FragmentsBuilder
    {
        /// <summary>
        /// create a fragment.
        /// </summary>
        /// <param name="reader">IndexReader of the index</param>
        /// <param name="docId">document id to be highlighted</param>
        /// <param name="fieldName">field of the document to be highlighted</param>
        /// <param name="fieldFragList">FieldFragList object</param>
        /// <param name="fragCharSize"></param>
        /// <returns>a created fragment or null when no fragment created</returns>
        string CreateFragment(IndexReader reader, int docId, string fieldName, FieldFragList fieldFragList, int fragCharSize, IState state);


        /// <summary>
        /// create multiple fragments.
        /// </summary>
        /// <param name="reader">IndexReader of the index</param>
        /// <param name="docId">document id to be highlighted</param>
        /// <param name="fieldName">field of the document to be highlighted</param>
        /// <param name="fieldFragList">FieldFragList object</param>
        /// <param name="maxNumFragments">maximum number of fragments</param>
        /// <param name="fragCharSize"></param>
        /// <returns>created fragments or null when no fragments created. Size of the array can be less than maxNumFragments</returns>
        string[] CreateFragments(IndexReader reader, int docId, string fieldName, FieldFragList fieldFragList, int maxNumFragments, int fragCharSize, IState state);
    }
}
