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

namespace Lucene.Net.Util
{
	
	/// <summary> Base test class for Lucene test classes that test Locale-sensitive behavior.
	/// <p/>
	/// This class will run tests under the default Locale, but then will also run
	/// tests under all available JVM locales. This is helpful to ensure tests will
	/// not fail under a different environment.
	/// </summary>
	public class LocalizedTestCase:LuceneTestCase
	{
		/// <summary> Before changing the default Locale, save the default Locale here so that it
		/// can be restored.
		/// </summary>
		private System.Globalization.CultureInfo defaultLocale = System.Threading.Thread.CurrentThread.CurrentCulture;
		
		/// <summary> The locale being used as the system default Locale</summary>
        private System.Globalization.CultureInfo locale = System.Globalization.CultureInfo.CurrentCulture;
		
		/// <summary> An optional limited set of testcases that will run under different Locales.</summary>
		private System.Collections.Hashtable testWithDifferentLocales;
		
		public LocalizedTestCase():base()
		{
			testWithDifferentLocales = null;
		}
		
		public LocalizedTestCase(System.String name):base(name)
		{
			testWithDifferentLocales = null;
		}
		
		public LocalizedTestCase(System.Collections.Hashtable testWithDifferentLocales):base()
		{
			this.testWithDifferentLocales = testWithDifferentLocales;
		}
		
		public LocalizedTestCase(System.String name, System.Collections.Hashtable testWithDifferentLocales):base(name)
		{
			this.testWithDifferentLocales = testWithDifferentLocales;
		}
		
		// @Override
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			System.Threading.Thread.CurrentThread.CurrentCulture = locale;
		}
		
		// @Override
		[TearDown]
		public override void  TearDown()
		{
			System.Threading.Thread.CurrentThread.CurrentCulture = defaultLocale;
			base.TearDown();
		}
		
		// @Override
		public override void  RunBare()
		{
			// Do the test with the default Locale (default)
			try
			{
				locale = defaultLocale;
				base.RunBare();
			}
			catch (System.Exception e)
			{
                System.Console.Out.WriteLine("Test failure of '" + Lucene.Net.TestCase.GetName() + "' occurred with the default Locale " + locale); 
				throw e;
			}

            if (testWithDifferentLocales == null || testWithDifferentLocales.Contains(Lucene.Net.TestCase.GetName())) 
			{
				// Do the test again under different Locales
				System.Globalization.CultureInfo[] systemLocales = System.Globalization.CultureInfo.GetCultures(System.Globalization.CultureTypes.InstalledWin32Cultures);
				for (int i = 0; i < systemLocales.Length; i++)
				{
					try
					{
						locale = systemLocales[i];
						base.RunBare();
					}
					catch (System.Exception e)
					{
                        System.Console.Out.WriteLine("Test failure of '" + Lucene.Net.TestCase.GetName() + "' occurred under a different Locale " + locale); // {{Aroush-2.9}} String junit.framework.TestCase.getName()
						throw e;
					}
				}
			}
		}
	}
}