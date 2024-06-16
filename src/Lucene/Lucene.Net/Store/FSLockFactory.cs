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
using System.IO;

namespace Lucene.Net.Store
{
	
	/// <summary> Base class for file system based locking implementation.</summary>
	
	public abstract class FSLockFactory:LockFactory
	{
		
		/// <summary> Directory for the lock files.</summary>
		protected internal System.IO.DirectoryInfo internalLockDir = null;

        /// <summary> Gets the lock directory.
        /// <para>Subclasses can use this to set the lock directory.
        /// This method can be only called
        /// once to initialize the lock directory. It is used by <see cref="FSDirectory" />
        /// to set the lock directory to itsself.
        /// Subclasses can also use this method to set the directory
        /// in the constructor.
        /// </para>
        /// </summary>
	    public virtual DirectoryInfo LockDir
	    {
	        get { return internalLockDir; }
	        protected internal set
	        {
	            if (this.internalLockDir != null)
	                throw new System.SystemException("You can set the lock directory for this factory only once.");
	            this.internalLockDir = value;
	        }
	    }
	}
}