/*
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
*/

//using System;
//using System.IO;
//using System.Collections;

//namespace Lucene.Net.Analysis
//{
//    /// <summary>
//    /// Loads a text file and adds every line as an entry to a Hashtable. Every line
//    /// should contain only one word. If the file is not found or on any error, an
//    /// empty table is returned.
//    /// </summary>
//    public class WordlistLoader
//    {
//        /// <summary>
//        /// Load words table from the file
//        /// </summary>
//        /// <param name="path">Path to the wordlist</param>
//        /// <param name="wordfile">Name of the wordlist</param>
//        /// <returns></returns>
//        public static Hashtable GetWordSet( String path, String wordfile ) 
//        {
//            if ( path == null || wordfile == null ) 
//            {
//                return new Hashtable();
//            }
//            return GetWordSet(new FileInfo(path + "\\" + wordfile));
//        }

//        /// <summary>
//        /// Load words table from the file
//        /// </summary>
//        /// <param name="wordfile">Complete path to the wordlist</param>
//        /// <returns></returns>
//        public static Hashtable GetWordSet( String wordfile ) 
//        {
//            if ( wordfile == null ) 
//            {
//                return new Hashtable();
//            }
//            return GetWordSet( new FileInfo( wordfile ) );
//        }

//        /// <summary>
//        /// Load words table from the file 
//        /// </summary>
//        /// <param name="wordfile">File containing the wordlist</param>
//        /// <returns></returns>
//        public static Hashtable GetWordSet( FileInfo wordfile ) 
//        {
//            if ( wordfile == null ) 
//            {
//                return new Hashtable();
//            }			
//            StreamReader lnr = new StreamReader(wordfile.FullName);
//            return GetWordSet(lnr);
//        }

//        /// <summary>
//        /// Reads lines from a Reader and adds every line as an entry to a HashSet (omitting
//        /// leading and trailing whitespace). Every line of the Reader should contain only
//        /// one word. The words need to be in lowercase if you make use of an
//        /// Analyzer which uses LowerCaseFilter (like StandardAnalyzer).
//        /// </summary>
//        /// <param name="reader">Reader containing the wordlist</param>
//        /// <returns>A Hashtable with the reader's words</returns>
//        public static Hashtable GetWordSet(TextReader reader)
//        {
//            Hashtable result = new Hashtable();			
//            try 
//            {				
//                ArrayList stopWords = new ArrayList();
//                String word = null;
//                while ( ( word = reader.ReadLine() ) != null ) 
//                {
//                    stopWords.Add(word.Trim());
//                }
//                result = MakeWordTable( (String[])stopWords.ToArray(typeof(string)), stopWords.Count);
//            }
//                // On error, use an empty table
//            catch (IOException) 
//            {
//                result = new Hashtable();
//            }
//            return result;
//        }


//        /// <summary>
//        /// Builds the wordlist table.
//        /// </summary>
//        /// <param name="words">Word that where read</param>
//        /// <param name="length">Amount of words that where read into <tt>words</tt></param>
//        /// <returns></returns>
//        private static Hashtable MakeWordTable( String[] words, int length ) 
//        {
//            Hashtable table = new Hashtable( length );
//            for ( int i = 0; i < length; i++ ) 
//            {
//                table.Add(words[i], words[i]);
//            }
//            return table;
//        }
//    }
//}
