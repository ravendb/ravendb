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

using Lucene.Net.Analysis.Tokenattributes;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Analysis
{
	
	/// <summary> Base class for all Lucene unit tests that use TokenStreams.  
	/// <p/>
	/// This class runs all tests twice, one time with {@link TokenStream#setOnlyUseNewAPI} <code>false</code>
	/// and after that one time with <code>true</code>.
	/// </summary>
	public abstract class BaseTokenStreamTestCase:LuceneTestCase
	{
		
		private bool onlyUseNewAPI = false;
		private System.Collections.Hashtable testWithNewAPI;
		
		public BaseTokenStreamTestCase():base()
		{
			this.testWithNewAPI = null; // run all tests also with onlyUseNewAPI
		}
		
		public BaseTokenStreamTestCase(System.String name):base(name)
		{
			this.testWithNewAPI = null; // run all tests also with onlyUseNewAPI
		}
		
		public BaseTokenStreamTestCase(System.Collections.Hashtable testWithNewAPI):base()
		{
			this.testWithNewAPI = testWithNewAPI;
		}
		
		public BaseTokenStreamTestCase(System.String name, System.Collections.Hashtable testWithNewAPI):base(name)
		{
			this.testWithNewAPI = testWithNewAPI;
		}
		
		// @Override
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			TokenStream.SetOnlyUseNewAPI(onlyUseNewAPI);
		}
		
		// @Override
		public override void  RunBare()
		{
			// Do the test with onlyUseNewAPI=false (default)
			try
			{
				onlyUseNewAPI = false;
				// base.RunBare();  // {{Aroush-2.9}}
                System.Diagnostics.Debug.Fail("Port issue:", "base.RunBare()"); // {{Aroush-2.9}}
			}
			catch (System.Exception e)
			{
				System.Console.Out.WriteLine("Test failure of '" + GetType() + "' occurred with onlyUseNewAPI=false");
				throw e;
			}
			
			if (testWithNewAPI == null || testWithNewAPI.Contains(GetType()))
			{
				// Do the test again with onlyUseNewAPI=true
				try
				{
					onlyUseNewAPI = true;
					base.RunBare();
				}
				catch (System.Exception e)
				{
					System.Console.Out.WriteLine("Test failure of '" + GetType() + "' occurred with onlyUseNewAPI=true");
					throw e;
				}
			}
		}
		
		// some helpers to test Analyzers and TokenStreams:

        public interface CheckClearAttributesAttribute : Lucene.Net.Util.Attribute
        {
               bool GetAndResetClearCalled();
        }

        public class CheckClearAttributesAttributeImpl : Lucene.Net.Util.AttributeImpl ,CheckClearAttributesAttribute 
        {
            private bool clearCalled = false;

            public bool GetAndResetClearCalled()
            {
                try
                {
                    return clearCalled;
                }
                finally
                {
                    clearCalled = false;
                }
            }

            //@Override
            public override void Clear()
            {
                clearCalled = true;
            }

            //@Override
            public  override bool Equals(Object other) 
            {
                return (
                other is CheckClearAttributesAttributeImpl &&
                ((CheckClearAttributesAttributeImpl) other).clearCalled == this.clearCalled
                );
            }


            //@Override
            public override int GetHashCode()
            {
                //Java: return 76137213 ^ Boolean.valueOf(clearCalled).hashCode();
                return 76137213 ^ clearCalled.GetHashCode();
            }

            //@Override
            public override void CopyTo(Lucene.Net.Util.AttributeImpl target)
            {
                ((CheckClearAttributesAttributeImpl)target).Clear();
            }
        }

        public static void AssertTokenStreamContents(TokenStream ts, System.String[] output, int[] startOffsets, int[] endOffsets, System.String[] types, int[] posIncrements, int? finalOffset)
        {
            Assert.IsNotNull(output);
            CheckClearAttributesAttribute checkClearAtt = (CheckClearAttributesAttribute)ts.AddAttribute(typeof(CheckClearAttributesAttribute));

            Assert.IsTrue(ts.HasAttribute(typeof(TermAttribute)), "has no TermAttribute");
            TermAttribute termAtt = (TermAttribute)ts.GetAttribute(typeof(TermAttribute));

            OffsetAttribute offsetAtt = null;
            if (startOffsets != null || endOffsets != null || finalOffset != null)
            {
                Assert.IsTrue(ts.HasAttribute(typeof(OffsetAttribute)), "has no OffsetAttribute");
                offsetAtt = (OffsetAttribute)ts.GetAttribute(typeof(OffsetAttribute));
            }
    
            TypeAttribute typeAtt = null;
            if (types != null)
            {
                Assert.IsTrue(ts.HasAttribute(typeof(TypeAttribute)), "has no TypeAttribute");
                typeAtt = (TypeAttribute)ts.GetAttribute(typeof(TypeAttribute));
            }
            
            PositionIncrementAttribute posIncrAtt = null;
            if (posIncrements != null)
            {
                Assert.IsTrue(ts.HasAttribute(typeof(PositionIncrementAttribute)), "has no PositionIncrementAttribute");
                posIncrAtt = (PositionIncrementAttribute)ts.GetAttribute(typeof(PositionIncrementAttribute));
            }

            ts.Reset();
            for (int i = 0; i < output.Length; i++)
            {
                // extra safety to enforce, that the state is not preserved and also assign bogus values
                ts.ClearAttributes();
                termAtt.SetTermBuffer("bogusTerm");
                if (offsetAtt != null) offsetAtt.SetOffset(14584724, 24683243);
                if (typeAtt != null) typeAtt.SetType("bogusType");
                if (posIncrAtt != null) posIncrAtt.SetPositionIncrement(45987657);

                checkClearAtt.GetAndResetClearCalled(); // reset it, because we called clearAttribute() before
                Assert.IsTrue(ts.IncrementToken(), "token " + i + " does not exist");
                Assert.IsTrue(checkClearAtt.GetAndResetClearCalled(), "clearAttributes() was not called correctly in TokenStream chain");

                Assert.AreEqual(output[i], termAtt.Term(), "term " + i);
                if (startOffsets != null)
                    Assert.AreEqual(startOffsets[i], offsetAtt.StartOffset(), "startOffset " + i);
                if (endOffsets != null)
                    Assert.AreEqual(endOffsets[i], offsetAtt.EndOffset(), "endOffset " + i);
                if (types != null)
                    Assert.AreEqual(types[i], typeAtt.Type(), "type " + i);
                if (posIncrements != null)
                    Assert.AreEqual(posIncrements[i], posIncrAtt.GetPositionIncrement(), "posIncrement " + i);
            }
            Assert.IsFalse(ts.IncrementToken(), "end of stream");
            ts.End();
            if (finalOffset.HasValue)
                Assert.AreEqual(finalOffset, offsetAtt.EndOffset(), "finalOffset ");
            ts.Close();
        }

        public static void AssertTokenStreamContents(TokenStream ts, String[] output, int[] startOffsets, int[] endOffsets, String[] types, int[] posIncrements)
        {
            AssertTokenStreamContents(ts, output, startOffsets, endOffsets, types, posIncrements, null);
        }

        public static void AssertTokenStreamContents(TokenStream ts, String[] output)
        {
            AssertTokenStreamContents(ts, output, null, null, null, null, null);
        }

        public static void AssertTokenStreamContents(TokenStream ts, String[] output, String[] types)
        {
            AssertTokenStreamContents(ts, output, null, null, types, null, null);
        }

        public static void AssertTokenStreamContents(TokenStream ts, String[] output, int[] posIncrements)
        {
            AssertTokenStreamContents(ts, output, null, null, null, posIncrements, null);
        }

        public static void AssertTokenStreamContents(TokenStream ts, String[] output, int[] startOffsets, int[] endOffsets)
        {
            AssertTokenStreamContents(ts, output, startOffsets, endOffsets, null, null, null);
        }

        public static void AssertTokenStreamContents(TokenStream ts, String[] output, int[] startOffsets, int[] endOffsets, int? finalOffset)
        {
            AssertTokenStreamContents(ts, output, startOffsets, endOffsets, null, null, finalOffset);
        }

        public static void AssertTokenStreamContents(TokenStream ts, String[] output, int[] startOffsets, int[] endOffsets, int[] posIncrements)
        {
            AssertTokenStreamContents(ts, output, startOffsets, endOffsets, null, posIncrements, null);
        }

        public static void AssertTokenStreamContents(TokenStream ts, String[] output, int[] startOffsets, int[] endOffsets, int[] posIncrements, int? finalOffset)
        {
            AssertTokenStreamContents(ts, output, startOffsets, endOffsets, null, posIncrements, finalOffset);
        }

        public static void AssertAnalyzesTo(Analyzer a, String input, String[] output, int[] startOffsets, int[] endOffsets, String[] types, int[] posIncrements)
        {
            AssertTokenStreamContents(a.TokenStream("dummy", new System.IO.StringReader(input)), output, startOffsets, endOffsets, types, posIncrements, input.Length);
        }

        public static void AssertAnalyzesTo(Analyzer a, String input, String[] output)
        {
            AssertAnalyzesTo(a, input, output, null, null, null, null);
        }

        public static void AssertAnalyzesTo(Analyzer a, String input, String[] output, String[] types)
        {
            AssertAnalyzesTo(a, input, output, null, null, types, null);
        }

        public static void AssertAnalyzesTo(Analyzer a, String input, String[] output, int[] posIncrements)
        {
            AssertAnalyzesTo(a, input, output, null, null, null, posIncrements);
        }

        public static void AssertAnalyzesTo(Analyzer a, String input, String[] output, int[] startOffsets, int[] endOffsets)
        {
            AssertAnalyzesTo(a, input, output, startOffsets, endOffsets, null, null);
        }

        public static void AssertAnalyzesTo(Analyzer a, String input, String[] output, int[] startOffsets, int[] endOffsets, int[] posIncrements)
        {
            AssertAnalyzesTo(a, input, output, startOffsets, endOffsets, null, posIncrements);
        }


        public static void AssertAnalyzesToReuse(Analyzer a, String input, String[] output, int[] startOffsets, int[] endOffsets, String[] types, int[] posIncrements)
        {
            AssertTokenStreamContents(a.ReusableTokenStream("dummy", new System.IO.StringReader(input)), output, startOffsets, endOffsets, types, posIncrements, input.Length);
        }

        public static void AssertAnalyzesToReuse(Analyzer a, String input, String[] output)
        {
            AssertAnalyzesToReuse(a, input, output, null, null, null, null);
        }

        public static void AssertAnalyzesToReuse(Analyzer a, String input, String[] output, String[] types)
        {
            AssertAnalyzesToReuse(a, input, output, null, null, types, null);
        }

        public static void AssertAnalyzesToReuse(Analyzer a, String input, String[] output, int[] posIncrements)
        {
            AssertAnalyzesToReuse(a, input, output, null, null, null, posIncrements);
        }

        public static void AssertAnalyzesToReuse(Analyzer a, String input, String[] output, int[] startOffsets, int[] endOffsets)
        {
            AssertAnalyzesToReuse(a, input, output, startOffsets, endOffsets, null, null);
        }

        public static void AssertAnalyzesToReuse(Analyzer a, String input, String[] output, int[] startOffsets, int[] endOffsets, int[] posIncrements)
        {
            AssertAnalyzesToReuse(a, input, output, startOffsets, endOffsets, null, posIncrements);
        }

		// simple utility method for testing stemmers
		
		public static void  CheckOneTerm(Analyzer a, System.String input, System.String expected)
		{
			AssertAnalyzesTo(a, input, new System.String[]{expected});
		}
		
		public static void  CheckOneTermReuse(Analyzer a, System.String input, System.String expected)
		{
			AssertAnalyzesToReuse(a, input, new System.String[]{expected});
		}
	}
}