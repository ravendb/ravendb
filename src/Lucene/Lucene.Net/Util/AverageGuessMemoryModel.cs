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

namespace Lucene.Net.Util
{
	
	/// <summary> An average, best guess, MemoryModel that should work okay on most systems.
	/// 
	/// </summary>
	public class AverageGuessMemoryModel:MemoryModel
	{
        public AverageGuessMemoryModel()
        {
            InitBlock();
        }

	    private void InitBlock()
	    {
	        sizes = new Dictionary<Type, int>(IdentityStructComparer<Type>.Default);
	        sizes[typeof(bool)] = 1;
            sizes[typeof (byte)] = 1;
            sizes[typeof(sbyte)] = 1;
            sizes[typeof (char)] = 2;
            sizes[typeof (short)] = 2;
            sizes[typeof (int)] = 4;
	        sizes[typeof(float)] = 4;
            sizes[typeof (double)] = 8;
            sizes[typeof (long)] = 8;
            sizes[typeof (UIntPtr)] = UIntPtr.Size;
            sizes[typeof (IntPtr)] = IntPtr.Size;
        }
		// best guess primitive sizes
        private Dictionary<Type, int> sizes;
		
		/*
		* (non-Javadoc)
		* 
		* <see cref="Lucene.Net.Util.MemoryModel.getArraySize()"/>
		*/

	    public override int ArraySize
	    {
	        get { return 16; }
	    }

	    /*
		* (non-Javadoc)
		* 
		* <see cref="Lucene.Net.Util.MemoryModel.getClassSize()"/>
		*/

	    public override int ClassSize
	    {
	        get { return 8; }
	    }

	    /* (non-Javadoc)
		* <see cref="Lucene.Net.Util.MemoryModel.getPrimitiveSize(java.lang.Class)"/>
		*/
		public override int GetPrimitiveSize(Type clazz)
		{
			return sizes[clazz];
		}
		
		/* (non-Javadoc)
		* <see cref="Lucene.Net.Util.MemoryModel.getReferenceSize()"/>
		*/

	    public override int ReferenceSize
	    {
	        get { return 4; }
	    }
	}
}