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

using System.Collections;
using Lucene.Net.Documents;

namespace SpellChecker.Net.Search.Spell
{
    using System;
    using IndexReader = Lucene.Net.Index.IndexReader;
    using TermEnum = Lucene.Net.Index.TermEnum;
    using Term = Lucene.Net.Index.Term;

    /// <summary> 
    /// Lucene Dictionary
    /// </summary>
    public class LuceneDictionary : IDictionary, System.Collections.Generic.IEnumerable<string>
    {
        internal IndexReader reader;
        internal System.String field;
		
        public LuceneDictionary(IndexReader reader, System.String field)
        {
            this.reader = reader;
            this.field = field;
        }

        virtual public System.Collections.Generic.IEnumerator<string> GetWordsIterator()
        {
            return new LuceneIterator(this);
        }

        public System.Collections.Generic.IEnumerator<string> GetEnumerator()
        {
            return GetWordsIterator();
        }
        
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
		
        internal sealed class LuceneIterator : System.Collections.Generic.IEnumerator<string>
        {
            private readonly TermEnum termEnum;
            private Term actualTerm;
            private bool hasNextCalled;

            private readonly LuceneDictionary enclosingInstance;
			
            public LuceneIterator(LuceneDictionary enclosingInstance)
            {
                this.enclosingInstance = enclosingInstance;
                try
                {
                    termEnum = enclosingInstance.reader.Terms(new Term(enclosingInstance.field, ""));
                }
                catch (System.IO.IOException ex)
                {
                    System.Console.Error.WriteLine(ex.StackTrace);
                }
            }

            public string Current
            {
                get
                {
                    if (!hasNextCalled)
                    {
                        MoveNext();
                    }
                    hasNextCalled = false;
                    return (actualTerm != null) ? actualTerm.Text : null;
                }

            }

            object IEnumerator.Current
            {
                get { return Current; }
            }
			
            public bool MoveNext()
            {
                hasNextCalled = true;
                
                actualTerm = termEnum.Term;

                // if there are no words return false
                if (actualTerm == null) return false;

                System.String fieldt = actualTerm.Field;
                termEnum.Next();

                // if the next word doesn't have the same field return false
                if (fieldt != enclosingInstance.field)
                {
                    actualTerm = null;
                    return false;
                }
                return true;
            }

            public void Remove()
            {

            }

            public void Reset()
            {

            }

            public void Dispose()
            {
                // Nothing
            }
        }
    }
}