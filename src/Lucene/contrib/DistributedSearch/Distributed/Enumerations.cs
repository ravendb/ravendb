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

namespace Lucene.Net.Distributed
{

    /// <summary>
    /// Specifies the location of a DistributedSearcher
    /// </summary>
	public enum SearchMethod
	{
		Local		= 0,
		Distributed	= 1,
		Undefined	= 2
	}

    /// <summary>
    /// Specifies the type of Field in an IndexDocument
    /// </summary>
	public enum FieldStorageType
	{
		Keyword		= 1,
		UnIndexed	= 2,
		UnStored	= 3,
		Text		= 4
	}

    /// <summary>
    /// Specifies the type of action for an IndexSet to take when applying changes to an index
    /// </summary>
    public enum IndexAction
    {
        NoAction = 0,
        Update = 1,
        Overwrite = 2
    }

    /// <summary>
    /// Specifies the type of Analyzer to use in creation of an IndexDocument
    /// </summary>
	public enum AnalyzerType
	{
		StandardAnalyzer			= 0,
		SimpleAnalyzer				= 1,
		WhitespaceAnalyzer			= 2,
		StopAnalyzer				= 3
	}

}
