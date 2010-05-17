/* ====================================================================
* The Apache Software License, Version 1.1
*
* Copyright (c) 2004 The Apache Software Foundation.  All rights
* reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions
* are met:
*
* 1. Redistributions of source code must retain the above copyright
*    notice, this list of conditions and the following disclaimer.
*
* 2. Redistributions in binary form must reproduce the above copyright
*    notice, this list of conditions and the following disclaimer in
*    the documentation and/or other materials provided with the
*    distribution.
*
* 3. The end-user documentation included with the redistribution,
*    if any, must include the following acknowledgment:
*       "This product includes software developed by the
*        Apache Software Foundation (http://www.apache.org/)."
*    Alternately, this acknowledgment may appear in the software itself,
*    if and wherever such third-party acknowledgments normally appear.
*
* 4. The names "Apache" and "Apache Software Foundation" and
*    "Apache Lucene" must not be used to endorse or promote products
*    derived from this software without prior written permission. For
*    written permission, please contact apache@apache.org.
*
* 5. Products derived from this software may not be called "Apache",
*    "Apache Lucene", nor may "Apache" appear in their name, without
*    prior written permission of the Apache Software Foundation.
*
* THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
* WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
* OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
* ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
* SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
* LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
* USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
* OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
* OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
* SUCH DAMAGE.
* ====================================================================
*
* This software consists of voluntary contributions made by many
* individuals on behalf of the Apache Software Foundation.  For more
* information on the Apache Software Foundation, please see
* <http://www.apache.org/>.
*/
using System;
// {{Aroush}} using junit.framework;
using Lucene.Net.Analysis;
namespace Lucene.Net.Analysis.Snowball
{
	
	public class TestSnowball
	{
		private class AnonymousClassTokenStream : TokenStream
		{
			public AnonymousClassTokenStream(Token tok, TestSnowball enclosingInstance)
			{
				InitBlock(tok, enclosingInstance);
			}
			private void  InitBlock(Token tok, TestSnowball enclosingInstance)
			{
				this.tok = tok;
				this.enclosingInstance = enclosingInstance;
			}
			private Token tok;
			private TestSnowball enclosingInstance;
			public TestSnowball Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
            public override Token Next()
			{
				return tok;
			}
		}
		
		public virtual void  AssertAnalyzesTo(Analyzer a, System.String input, System.String[] output)
		{
			TokenStream ts = a.TokenStream("dummy", new System.IO.StringReader(input));
			for (int i = 0; i < output.Length; i++)
			{
				Token t = ts.Next();
				System.Diagnostics.Trace.Assert(t != null); ////   assertNotNull(t);
				System.Diagnostics.Trace.Assert(output[i] == t.TermText()); //// assertEquals(output[i], t.TermText());
			}
			System.Diagnostics.Trace.Assert(ts.Next() == null); //// assertNull(ts.Next());
			ts.Close();
		}
		
		public virtual void  TestEnglish()
		{
			Analyzer a = new SnowballAnalyzer("English");
			AssertAnalyzesTo(a, "he abhorred accents", new System.String[]{"he", "abhor", "accent"});
		}
		
		public virtual void  TestFilterTokens()
		{
			Token tok = new Token("accents", 2, 7, "wrd");
			tok.SetPositionIncrement(3);
			
			SnowballFilter filter = new SnowballFilter(new AnonymousClassTokenStream(tok, this), "English");
			
			Token newtok = filter.Next();
			
			System.Diagnostics.Trace.Assert("accent" == newtok.TermText()); //// assertEquals("accent", newtok.TermText());
			System.Diagnostics.Trace.Assert(2 == newtok.StartOffset()); //// assertEquals(2, newtok.StartOffset());
			System.Diagnostics.Trace.Assert(7 == newtok.EndOffset()); //// assertEquals(7, newtok.EndOffset());
			System.Diagnostics.Trace.Assert("wrd" == newtok.Type()); //// assertEquals("wrd", newtok.Type());
			System.Diagnostics.Trace.Assert(3 == newtok.GetPositionIncrement()); //// assertEquals(3, newtok.GetPositionIncrement());
		}

        //// {{Aroush}}
        [STAThread]
        public static void  Main(System.String[] args)
        {
            TestSnowball t = new TestSnowball();

            t.TestEnglish();
            t.TestFilterTokens();
        }
        //// {{Aroush}}
	}
}
