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

namespace Lucene.Net.Messages
{
	
	public class MessagesTestBundle:NLS
	{
		
		private static readonly System.String BUNDLE_NAME;
		
		private MessagesTestBundle()
		{
			// should never be instantiated
		}
		
		// static string must match the strings in the property files.
		public static System.String Q0001E_INVALID_SYNTAX;
		public static System.String Q0004E_INVALID_SYNTAX_ESCAPE_UNICODE_TRUNCATION;
		
		// this message is missing from the properties file
		public static System.String Q0005E_MESSAGE_NOT_IN_BUNDLE;
		static MessagesTestBundle()
		{
			BUNDLE_NAME = typeof(MessagesTestBundle).FullName;
			{
				// register all string ids with NLS class and initialize static string
				// values
				NLS.InitializeMessages(BUNDLE_NAME, typeof(MessagesTestBundle));
			}
		}
	}
}