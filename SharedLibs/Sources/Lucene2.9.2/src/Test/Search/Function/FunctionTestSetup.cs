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

using Analyzer = Lucene.Net.Analysis.Analyzer;
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using Fieldable = Lucene.Net.Documents.Fieldable;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search.Function
{
	
	/// <summary> Setup for function tests</summary>

	public abstract class FunctionTestSetup:LuceneTestCase
	{
		
		/// <summary> Actual score computation order is slightly different than assumptios
		/// this allows for a small amount of variation
		/// </summary>
		public static float TEST_SCORE_TOLERANCE_DELTA = 0.001f;
		
		protected internal const bool DBG = false; // change to true for logging to print
		
		protected internal const int N_DOCS = 17; // select a primary number > 2
		
		protected internal const System.String ID_FIELD = "id";
		protected internal const System.String TEXT_FIELD = "text";
		protected internal const System.String INT_FIELD = "iii";
		protected internal const System.String FLOAT_FIELD = "fff";
		
		private static readonly System.String[] DOC_TEXT_LINES = new System.String[]{"Well, this is just some plain text we use for creating the ", "test documents. It used to be a text from an online collection ", "devoted to first aid, but if there was there an (online) lawyers ", "first aid collection with legal advices, \"it\" might have quite ", "probably advised one not to include \"it\"'s text or the text of ", "any other online collection in one's code, unless one has money ", "that one don't need and one is happy to donate for lawyers ", "charity. Anyhow at some point, rechecking the usage of this text, ", "it became uncertain that this text is free to use, because ", "the web site in the disclaimer of he eBook containing that text ", "was not responding anymore, and at the same time, in projGut, ", "searching for first aid no longer found that eBook as well. ", "So here we are, with a perhaps much less interesting ", "text for the test, but oh much much safer. "};
		
		protected internal Directory dir;
		protected internal Analyzer anlzr;
		
		/* @override constructor */
		public FunctionTestSetup(System.String name):this(name, false)
		{
		}
        
        private bool doMultiSegment;

        public FunctionTestSetup(String name, bool doMultiSegment) : base(name)
        {
            this.doMultiSegment = doMultiSegment;
        }


        public FunctionTestSetup()
            : base()
        {
        }
		/* @override */
		[TearDown]
		public override void  TearDown()
		{
			base.TearDown();
			dir = null;
			anlzr = null;
		}
		
		/* @override */
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			// prepare a small index with just a few documents.  
			base.SetUp();
			dir = new RAMDirectory();
			anlzr = new StandardAnalyzer();
			IndexWriter iw = new IndexWriter(dir, anlzr, IndexWriter.MaxFieldLength.LIMITED);
			// add docs not exactly in natural ID order, to verify we do check the order of docs by scores
			int remaining = N_DOCS;
			bool[] done = new bool[N_DOCS];
			int i = 0;
			while (remaining > 0)
			{
				if (done[i])
				{
					throw new System.Exception("to set this test correctly N_DOCS=" + N_DOCS + " must be primary and greater than 2!");
				}
				AddDoc(iw, i);
				done[i] = true;
				i = (i + 4) % N_DOCS;
                if (doMultiSegment && remaining % 3 == 0) 
                {
                    iw.Commit();
                }
				remaining--;
			}
			iw.Close();
		}
		
		private void  AddDoc(IndexWriter iw, int i)
		{
			Document d = new Document();
			Fieldable f;
			int scoreAndID = i + 1;
			
			f = new Field(ID_FIELD, Id2String(scoreAndID), Field.Store.YES, Field.Index.NOT_ANALYZED); // for debug purposes
			f.SetOmitNorms(true);
			d.Add(f);
			
			f = new Field(TEXT_FIELD, "text of doc" + scoreAndID + TextLine(i), Field.Store.NO, Field.Index.ANALYZED); // for regular search
			f.SetOmitNorms(true);
			d.Add(f);
			
			f = new Field(INT_FIELD, "" + scoreAndID, Field.Store.NO, Field.Index.NOT_ANALYZED); // for function scoring
			f.SetOmitNorms(true);
			d.Add(f);
			
			f = new Field(FLOAT_FIELD, scoreAndID + ".000", Field.Store.NO, Field.Index.NOT_ANALYZED); // for function scoring
			f.SetOmitNorms(true);
			d.Add(f);
			
			iw.AddDocument(d);
			Log("added: " + d);
		}
		
		// 17 --> ID00017
		protected internal virtual System.String Id2String(int scoreAndID)
		{
			System.String s = "000000000" + scoreAndID;
			int n = ("" + N_DOCS).Length + 3;
			int k = s.Length - n;
			return "ID" + s.Substring(k);
		}
		
		// some text line for regular search
		private System.String TextLine(int docNum)
		{
			return DOC_TEXT_LINES[docNum % DOC_TEXT_LINES.Length];
		}
		
		// extract expected doc score from its ID Field: "ID7" --> 7.0
		protected internal virtual float ExpectedFieldScore(System.String docIDFieldVal)
		{
            return SupportClass.Single.Parse(docIDFieldVal.Substring(2));
		}
		
		// debug messages (change DBG to true for anything to print) 
		protected internal virtual void  Log(System.Object o)
		{
			if (DBG)
			{
				System.Console.Out.WriteLine(o.ToString());
			}
		}
	}
}