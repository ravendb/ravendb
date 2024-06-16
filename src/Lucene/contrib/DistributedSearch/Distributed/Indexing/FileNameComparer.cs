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
using System.IO;

namespace Lucene.Net.Distributed.Indexing
{
	/// <summary>
	/// Summary description for FileNameComparer.
	/// </summary>
	public class FileNameComparer : IComparer
	{

		public int Compare(object x, object y)
		{
			if ((x is FileInfo) && (y is FileInfo))
			{
				FileInfo fX = (FileInfo)x;
				FileInfo fY = (FileInfo)y;
				return fX.Name.CompareTo(fY.Name);
			}
			else
			{
				return 0;
			}
		}
	}
}
