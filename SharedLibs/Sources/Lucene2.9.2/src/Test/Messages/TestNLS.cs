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

using NUnit.Framework;

namespace Lucene.Net.Messages
{
	
	
	[TestFixture]
	public class TestNLS
	{
		[Test]
		public virtual void  TestMessageLoading()
		{
			Message invalidSyntax = new MessageImpl(MessagesTestBundle.Q0001E_INVALID_SYNTAX, new System.Object[]{"XXX"});
            Assert.AreEqual("Syntax Error: XXX", invalidSyntax.GetLocalizedMessage());
		}
		
		[Test]
		public virtual void  TestMessageLoading_ja()
		{
			Message invalidSyntax = new MessageImpl(MessagesTestBundle.Q0001E_INVALID_SYNTAX, new System.Object[]{"XXX"});
			Assert.AreEqual("構文エラー: XXX", invalidSyntax.GetLocalizedMessage(new System.Globalization.CultureInfo("ja")));
		}
		
		[Test]
		public virtual void  TestNLSLoading()
		{
			System.String message = NLS.GetLocalizedMessage(MessagesTestBundle.Q0004E_INVALID_SYNTAX_ESCAPE_UNICODE_TRUNCATION);
			Assert.AreEqual("Truncated unicode escape sequence.", message);
			
			message = NLS.GetLocalizedMessage(MessagesTestBundle.Q0001E_INVALID_SYNTAX, new System.Object[]{"XXX"});
			Assert.AreEqual("Syntax Error: XXX", message);
		}
		
		[Test]
		public virtual void  TestNLSLoading_ja()
		{
			System.String message = NLS.GetLocalizedMessage(MessagesTestBundle.Q0004E_INVALID_SYNTAX_ESCAPE_UNICODE_TRUNCATION, new System.Globalization.CultureInfo("ja"));
			Assert.AreEqual("切り捨てられたユニコード・エスケープ・シーケンス。", message);
			
			message = NLS.GetLocalizedMessage(MessagesTestBundle.Q0001E_INVALID_SYNTAX, new System.Globalization.CultureInfo("ja"), new System.Object[]{"XXX"});
			Assert.AreEqual("構文エラー: XXX", message);
		}
		
		[Test]
		public virtual void  TestNLSLoading_xx_XX()
		{
            System.Globalization.CultureInfo locale;
            try
            {
                locale = new System.Globalization.CultureInfo("xx" + "-" + "XX");
            }
            catch
            {
                locale = System.Threading.Thread.CurrentThread.CurrentUICulture;
            }
			System.String message = NLS.GetLocalizedMessage(MessagesTestBundle.Q0004E_INVALID_SYNTAX_ESCAPE_UNICODE_TRUNCATION, locale);
			Assert.AreEqual("Truncated unicode escape sequence.", message);
			
			message = NLS.GetLocalizedMessage(MessagesTestBundle.Q0001E_INVALID_SYNTAX, locale, new System.Object[]{"XXX"});
			Assert.AreEqual("Syntax Error: XXX", message);
		}
		
		[Test]
		public virtual void  TestMissingMessage()
		{
			System.Globalization.CultureInfo locale = new System.Globalization.CultureInfo("en");
			System.String message = NLS.GetLocalizedMessage(MessagesTestBundle.Q0005E_MESSAGE_NOT_IN_BUNDLE, locale);
			
			Assert.AreEqual("Message with key:Q0005E_MESSAGE_NOT_IN_BUNDLE and locale: " + locale.ToString() + " not found.", message);
		}
	}
}