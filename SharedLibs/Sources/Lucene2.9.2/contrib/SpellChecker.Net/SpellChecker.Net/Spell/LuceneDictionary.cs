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
using IndexReader = Lucene.Net.Index.IndexReader;
using TermEnum = Lucene.Net.Index.TermEnum;
using Term = Lucene.Net.Index.Term;

namespace SpellChecker.Net.Search.Spell
{
	
    /// <summary> Lucene Dictionary
    /// 
    /// </summary>
    /// <author>  Nicolas Maisonneuve
    /// </author>
    public class LuceneDictionary : Dictionary
    {
        virtual public System.Collections.IEnumerator GetWordsIterator()
        {
            return new LuceneIterator(this);
        }
        internal IndexReader reader;
        internal System.String field;
		
        public LuceneDictionary(IndexReader reader, System.String field)
        {
            this.reader = reader;
            this.field = field;
        }
		
		
        internal sealed class LuceneIterator : System.Collections.IEnumerator
        {
            private void  InitBlock(LuceneDictionary enclosingInstance)
            {
                this.enclosingInstance = enclosingInstance;
            }
            private LuceneDictionary enclosingInstance;
            public System.Object Current
            {
                get
                {
                    if (!has_next_called)
                    {
                        MoveNext();
                    }
                    has_next_called = false;
                    return (actualTerm != null) ? actualTerm.Text() : null;
                }
				
            }
            public LuceneDictionary Enclosing_Instance
            {
                get
                {
                    return enclosingInstance;
                }
				
            }
            private TermEnum termEnum;
            private Term actualTerm;
            private bool has_next_called;
			
            public LuceneIterator(LuceneDictionary enclosingInstance)
            {
                InitBlock(enclosingInstance);
                try
                {
                    termEnum = Enclosing_Instance.reader.Terms(new Term(Enclosing_Instance.field, ""));
                }
                catch (System.IO.IOException ex)
                {
                    System.Console.Error.WriteLine(ex.StackTrace);
                }
            }
			
			
            public bool MoveNext()
            {
                has_next_called = true;
                try
                {
                    // if there is still words
                    if (!termEnum.Next())
                    {
                        actualTerm = null;
                        return false;
                    }
                    //  if the next word are in the field
                    actualTerm = termEnum.Term();
                    System.String fieldt = actualTerm.Field();
                    if ( fieldt != Enclosing_Instance.field)
                    {
                        actualTerm = null;
                        return false;
                    }
                    return true;
                }
                catch (System.IO.IOException ex)
                {
                    System.Console.Error.WriteLine(ex.StackTrace);
                    return false;
                }
            }
			
            public void  Remove()
            {
            }
			
            public void  Reset()
            {
            }
        }
    }
}