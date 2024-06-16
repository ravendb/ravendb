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

namespace SpellChecker.Net.Search.Spell
{
	
	
    /// <summary> Dictionary represented by a file text.
    /// <p/>Format allowed: 1 word per line:<br/>
    /// word1<br/>
    /// word2<br/>
    /// word3<br/>
    /// 
    /// </summary>
    /// <author>  Nicolas Maisonneuve
    /// </author>
    public class PlainTextDictionary : IDictionary, System.Collections.Generic.IEnumerable<string>
    {
        virtual public System.Collections.Generic.IEnumerator<string> GetWordsIterator()
        {
            return new FileIterator(this);
        }

        public System.Collections.Generic.IEnumerator<string> GetEnumerator()
        {
            return GetWordsIterator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
		
        private System.IO.StreamReader in_Renamed;
        private System.String line;
        private bool has_next_called;
		
        public PlainTextDictionary(System.IO.FileInfo file)
        {
            in_Renamed = new System.IO.StreamReader(new System.IO.StreamReader(file.FullName, System.Text.Encoding.Default).BaseStream, new System.IO.StreamReader(file.FullName, System.Text.Encoding.Default).CurrentEncoding);
        }
		
        public PlainTextDictionary(System.IO.Stream dictFile)
        {
            in_Renamed = new System.IO.StreamReader(new System.IO.StreamReader(dictFile, System.Text.Encoding.Default).BaseStream, new System.IO.StreamReader(dictFile, System.Text.Encoding.Default).CurrentEncoding);
        }


        internal sealed class FileIterator : System.Collections.Generic.IEnumerator<string>
        {
            public FileIterator(PlainTextDictionary enclosingInstance)
            {
                InitBlock(enclosingInstance);
            }

            private void InitBlock(PlainTextDictionary enclosingInstance)
            {
                this.enclosingInstance = enclosingInstance;
            }

            private PlainTextDictionary enclosingInstance;

            public string Current
            {
                get
                {
                    if (!Enclosing_Instance.has_next_called)
                    {
                        MoveNext();
                    }
                    Enclosing_Instance.has_next_called = false;
                    return Enclosing_Instance.line;
                }
            }

            object System.Collections.IEnumerator.Current
            {
                get
                {
                    if (!Enclosing_Instance.has_next_called)
                    {
                        MoveNext();
                    }
                    Enclosing_Instance.has_next_called = false;
                    return Enclosing_Instance.line;
                }

            }

            public PlainTextDictionary Enclosing_Instance
            {
                get { return enclosingInstance; }

            }

            public bool MoveNext()
            {
                Enclosing_Instance.has_next_called = true;
                try
                {
                    Enclosing_Instance.line = Enclosing_Instance.in_Renamed.ReadLine();
                }
                catch (System.IO.IOException ex)
                {
                    System.Console.Error.WriteLine(ex.StackTrace);
                    Enclosing_Instance.line = null;
                    return false;
                }
                return (Enclosing_Instance.line != null) ? true : false;
            }


            public void Remove()
            {
            }

            public void Reset()
            {
            }

            public void Dispose()
            {

            }
        }
    }
}