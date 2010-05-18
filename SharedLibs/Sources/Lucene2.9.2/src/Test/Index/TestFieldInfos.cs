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

using Document = Lucene.Net.Documents.Document;
using IndexOutput = Lucene.Net.Store.IndexOutput;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
	//import org.cnlp.utils.properties.ResourceBundleHelper;
	
	[TestFixture]
	public class TestFieldInfos:LuceneTestCase
	{
		
		private Document testDoc = new Document();
		
		public TestFieldInfos(System.String s):base(s)
		{
		}

        public TestFieldInfos() : base("")
        {
        }
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			DocHelper.SetupDoc(testDoc);
		}
		
		[Test]
		public virtual void  Test()
		{
			//Positive test of FieldInfos
			Assert.IsTrue(testDoc != null);
			FieldInfos fieldInfos = new FieldInfos();
			fieldInfos.Add(testDoc);
			//Since the complement is stored as well in the fields map
			Assert.IsTrue(fieldInfos.Size() == DocHelper.all.Count); //this is all b/c we are using the no-arg constructor
			RAMDirectory dir = new RAMDirectory();
			System.String name = "testFile";
			IndexOutput output = dir.CreateOutput(name);
			Assert.IsTrue(output != null);
			//Use a RAMOutputStream
			
			try
			{
				fieldInfos.Write(output);
				output.Close();
				Assert.IsTrue(output.Length() > 0);
				FieldInfos readIn = new FieldInfos(dir, name);
				Assert.IsTrue(fieldInfos.Size() == readIn.Size());
				FieldInfo info = readIn.FieldInfo("textField1");
				Assert.IsTrue(info != null);
				Assert.IsTrue(info.storeTermVector_ForNUnit == false);
				Assert.IsTrue(info.omitNorms_ForNUnit == false);
				
				info = readIn.FieldInfo("textField2");
				Assert.IsTrue(info != null);
				Assert.IsTrue(info.storeTermVector_ForNUnit == true);
				Assert.IsTrue(info.omitNorms_ForNUnit == false);
				
				info = readIn.FieldInfo("textField3");
				Assert.IsTrue(info != null);
				Assert.IsTrue(info.storeTermVector_ForNUnit == false);
				Assert.IsTrue(info.omitNorms_ForNUnit == true);
				
				info = readIn.FieldInfo("omitNorms");
				Assert.IsTrue(info != null);
				Assert.IsTrue(info.storeTermVector_ForNUnit == false);
				Assert.IsTrue(info.omitNorms_ForNUnit == true);
				
				dir.Close();
			}
			catch (System.IO.IOException e)
			{
				Assert.IsTrue(false);
			}
		}
	}
}