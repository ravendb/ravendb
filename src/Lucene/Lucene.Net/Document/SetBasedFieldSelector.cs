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
using System.Collections.Generic;

namespace Lucene.Net.Documents
{
    /// <summary> Declare what fields to load normally and what fields to load lazily
    /// 
    /// 
    /// </summary>
    [Serializable]
    public class SetBasedFieldSelector : FieldSelector
	{
		private ISet<string> fieldsToLoad;
		private ISet<string> lazyFieldsToLoad;
		
		/// <summary> Pass in the Set of <see cref="Field" /> names to load and the Set of <see cref="Field" /> names to load lazily.  If both are null, the
		/// Document will not have any <see cref="Field" /> on it.  
		/// </summary>
		/// <param name="fieldsToLoad">A Set of <see cref="String" /> field names to load.  May be empty, but not null
		/// </param>
		/// <param name="lazyFieldsToLoad">A Set of <see cref="String" /> field names to load lazily.  May be empty, but not null  
		/// </param>
		public SetBasedFieldSelector(ISet<string> fieldsToLoad, ISet<string> lazyFieldsToLoad)
		{
			this.fieldsToLoad = fieldsToLoad;
			this.lazyFieldsToLoad = lazyFieldsToLoad;
		}

        /// <summary> Indicate whether to load the field with the given name or not. If the <see cref="AbstractField.Name()" /> is not in either of the 
		/// initializing Sets, then <see cref="Lucene.Net.Documents.FieldSelectorResult.NO_LOAD" /> is returned.  If a Field name
		/// is in both <c>fieldsToLoad</c> and <c>lazyFieldsToLoad</c>, lazy has precedence.
		/// 
		/// </summary>
		/// <param name="fieldName">The <see cref="Field" /> name to check
		/// </param>
		/// <returns> The <see cref="FieldSelectorResult" />
		/// </returns>
		public virtual FieldSelectorResult Accept(System.String fieldName)
		{
			FieldSelectorResult result = FieldSelectorResult.NO_LOAD;
			if (fieldsToLoad.Contains(fieldName) == true)
			{
				result = FieldSelectorResult.LOAD;
			}
			if (lazyFieldsToLoad.Contains(fieldName) == true)
			{
				result = FieldSelectorResult.LAZY_LOAD;
			}
			return result;
		}
	}
}