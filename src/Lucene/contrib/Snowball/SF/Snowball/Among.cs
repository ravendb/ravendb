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
namespace SF.Snowball
{
	
	public class Among
	{
		public Among(System.String s, int substring_i, int result, System.String methodname, SnowballProgram methodobject)
		{
			this.s_size = s.Length;
			this.s = s;
			this.substring_i = substring_i;
			this.result = result;
			this.methodobject = methodobject;
			if (methodname.Length == 0)
			{
				this.method = null;
			}
			else
			{
				try
				{
					this.method = methodobject.GetType().GetMethod(methodname, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.DeclaredOnly, null, new System.Type[0], null);
				}
				catch (System.MethodAccessException)
				{
					// FIXME - debug message
					this.method = null;
				}
			}
		}
		
		public int s_size; /* search string */
		public System.String s; /* search string */
		public int substring_i; /* index to longest matching substring */
		public int result; /* result of the lookup */
		public System.Reflection.MethodInfo method; /* method to use if substring matches */
		public SnowballProgram methodobject; /* object to invoke method on */
	}
	
}