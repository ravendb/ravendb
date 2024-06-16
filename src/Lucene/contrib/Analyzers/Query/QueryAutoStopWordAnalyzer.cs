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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Lucene.Net.Index;
using Lucene.Net.Support;
using Lucene.Net.Util;
using Version = Lucene.Net.Util.Version;

namespace Lucene.Net.Analysis.Query
{
/*
 * An {@link Analyzer} used primarily at query time to wrap another analyzer and provide a layer of protection
 * which prevents very common words from being passed into queries. 
 * <p>
 * For very large indexes the cost
 * of reading TermDocs for a very common word can be  high. This analyzer was created after experience with
 * a 38 million doc index which had a term in around 50% of docs and was causing TermQueries for 
 * this term to take 2 seconds.
 * </p>
 * <p>
 * Use the various "addStopWords" methods in this class to automate the identification and addition of 
 * stop words found in an already existing index.
 * </p>
 */
public class QueryAutoStopWordAnalyzer : Analyzer {
  Analyzer _delegate;
  HashMap<String,ISet<String>> stopWordsPerField = new HashMap<String,ISet<String>>();
  //The default maximum percentage (40%) of index documents which
  //can contain a term, after which the term is considered to be a stop word.
  public const float defaultMaxDocFreqPercent = 0.4f;
  private readonly Version matchVersion;

  /*
   * Initializes this analyzer with the Analyzer object that actually produces the tokens
   *
   * @param _delegate The choice of {@link Analyzer} that is used to produce the token stream which needs filtering
   */
  public QueryAutoStopWordAnalyzer(Version matchVersion, Analyzer _delegate) 
  {
    this._delegate = _delegate;
    SetOverridesTokenStreamMethod<QueryAutoStopWordAnalyzer>();
    this.matchVersion = matchVersion;
  }

  /*
   * Automatically adds stop words for all fields with terms exceeding the defaultMaxDocFreqPercent
   *
   * @param reader The {@link IndexReader} which will be consulted to identify potential stop words that
   *               exceed the required document frequency
   * @return The number of stop words identified.
   * @throws IOException
   */
  public int AddStopWords(IndexReader reader) 
  {
    return AddStopWords(reader, defaultMaxDocFreqPercent);
  }

  /*
   * Automatically adds stop words for all fields with terms exceeding the maxDocFreqPercent
   *
   * @param reader     The {@link IndexReader} which will be consulted to identify potential stop words that
   *                   exceed the required document frequency
   * @param maxDocFreq The maximum number of index documents which can contain a term, after which
   *                   the term is considered to be a stop word
   * @return The number of stop words identified.
   * @throws IOException
   */
  public int AddStopWords(IndexReader reader, int maxDocFreq) 
  {
    int numStopWords = 0;
    ICollection<String> fieldNames = reader.GetFieldNames(IndexReader.FieldOption.INDEXED);
    for (IEnumerator<String> iter = fieldNames.GetEnumerator(); iter.MoveNext();) {
      String fieldName = iter.Current;
      numStopWords += AddStopWords(reader, fieldName, maxDocFreq);
    }
    return numStopWords;
  }

  /*
   * Automatically adds stop words for all fields with terms exceeding the maxDocFreqPercent
   *
   * @param reader        The {@link IndexReader} which will be consulted to identify potential stop words that
   *                      exceed the required document frequency
   * @param maxPercentDocs The maximum percentage (between 0.0 and 1.0) of index documents which
   *                      contain a term, after which the word is considered to be a stop word.
   * @return The number of stop words identified.
   * @throws IOException
   */
  public int AddStopWords(IndexReader reader, float maxPercentDocs) 
  {
    int numStopWords = 0;
    ICollection<String> fieldNames = reader.GetFieldNames(IndexReader.FieldOption.INDEXED);
    for (IEnumerator<String> iter = fieldNames.GetEnumerator(); iter.MoveNext();) {
      String fieldName = iter.Current;
      numStopWords += AddStopWords(reader, fieldName, maxPercentDocs);
    }
    return numStopWords;
  }

  /*
   * Automatically adds stop words for the given field with terms exceeding the maxPercentDocs
   *
   * @param reader         The {@link IndexReader} which will be consulted to identify potential stop words that
   *                       exceed the required document frequency
   * @param fieldName      The field for which stopwords will be added
   * @param maxPercentDocs The maximum percentage (between 0.0 and 1.0) of index documents which
   *                       contain a term, after which the word is considered to be a stop word.
   * @return The number of stop words identified.
   * @throws IOException
   */
  public int AddStopWords(IndexReader reader, String fieldName, float maxPercentDocs) 
  {
    return AddStopWords(reader, fieldName, (int) (reader.NumDocs() * maxPercentDocs));
  }

  /*
   * Automatically adds stop words for the given field with terms exceeding the maxPercentDocs
   *
   * @param reader     The {@link IndexReader} which will be consulted to identify potential stop words that
   *                   exceed the required document frequency
   * @param fieldName  The field for which stopwords will be added
   * @param maxDocFreq The maximum number of index documents which
   *                   can contain a term, after which the term is considered to be a stop word.
   * @return The number of stop words identified.
   * @throws IOException
   */
  public int AddStopWords(IndexReader reader, String fieldName, int maxDocFreq) 
  {
      var stopWords = Support.Compatibility.SetFactory.CreateHashSet<string>();
    String internedFieldName = StringHelper.Intern(fieldName);
    TermEnum te = reader.Terms(new Term(fieldName));
    Term term = te.Term;
    while (term != null) {
      if (term.Field != internedFieldName) {
        break;
      }
      if (te.DocFreq() > maxDocFreq) {
        stopWords.Add(term.Text);
      }
      if (!te.Next()) {
        break;
      }
      term = te.Term;
    }
    stopWordsPerField.Add(fieldName, stopWords);
    
    /* if the stopwords for a field are changed,
     * then saved streams for that field are erased.
     */
    IDictionary<String,SavedStreams> streamMap = (IDictionary<String,SavedStreams>) PreviousTokenStream;
    if (streamMap != null)
      streamMap.Remove(fieldName);
    
    return stopWords.Count;
  }

  public override TokenStream TokenStream(String fieldName, TextReader reader) {
    TokenStream result;
    try {
      result = _delegate.ReusableTokenStream(fieldName, reader);
    } catch (IOException) {
      result = _delegate.TokenStream(fieldName, reader);
    }
    var stopWords = stopWordsPerField[fieldName];
    if (stopWords != null) {
      result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                              result, stopWords);
    }
    return result;
  }
  
  private class SavedStreams {
    /* the underlying stream */
    protected internal TokenStream Wrapped;

    /*
     * when there are no stopwords for the field, refers to wrapped.
     * if there stopwords, it is a StopFilter around wrapped.
     */
    protected internal TokenStream WithStopFilter;
  };
  
  public override TokenStream ReusableTokenStream(String fieldName, TextReader reader)
{
    if (overridesTokenStreamMethod) {
      // LUCENE-1678: force fallback to tokenStream() if we
      // have been subclassed and that subclass overrides
      // tokenStream but not reusableTokenStream
      return TokenStream(fieldName, reader);
    }

    /* map of SavedStreams for each field */
    IDictionary<String, SavedStreams> streamMap = (IDictionary<String, SavedStreams>)PreviousTokenStream;
    if (streamMap == null) {
      streamMap = new HashMap<String, SavedStreams>();
      PreviousTokenStream = streamMap;
    }

    SavedStreams streams = streamMap[fieldName];
    if (streams == null) {
      /* an entry for this field does not exist, create one */
      streams = new SavedStreams();
      streamMap.Add(fieldName, streams);
      streams.Wrapped = _delegate.ReusableTokenStream(fieldName, reader);

      /* if there are any stopwords for the field, save the stopfilter */
      var stopWords = stopWordsPerField[fieldName];
      if (stopWords != null)
        streams.WithStopFilter = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                                streams.Wrapped, stopWords);
      else
        streams.WithStopFilter = streams.Wrapped;

    } else {
      /*
       * an entry for this field exists, verify the wrapped stream has not
       * changed. if it has not, reuse it, otherwise wrap the new stream.
       */
      TokenStream result = _delegate.ReusableTokenStream(fieldName, reader);
      if (result == streams.Wrapped) {
        /* the wrapped analyzer reused the stream */
        streams.WithStopFilter.Reset();
      } else {
        /*
         * the wrapped analyzer did not. if there are any stopwords for the
         * field, create a new StopFilter around the new stream
         */
        streams.Wrapped = result;
        var stopWords = stopWordsPerField[fieldName];
        if (stopWords != null)
          streams.WithStopFilter = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                                  streams.Wrapped, stopWords);
        else
          streams.WithStopFilter = streams.Wrapped;
      }
    }

    return streams.WithStopFilter;
  }

  /*
   * Provides information on which stop words have been identified for a field
   *
   * @param fieldName The field for which stop words identified in "addStopWords"
   *                  method calls will be returned
   * @return the stop words identified for a field
   */
  public String[] GetStopWords(String fieldName) {
    String[] result;
    var stopWords = stopWordsPerField[fieldName];
    if (stopWords != null) {
      result = stopWords.ToArray();
    } else {
      result = new String[0];
    }
    return result;
  }

  /*
   * Provides information on which stop words have been identified for all fields
   *
   * @return the stop words (as terms)
   */
  public Term[] GetStopWords() {
    List<Term> allStopWords = new List<Term>();
    foreach(var fieldName in stopWordsPerField.Keys) 
    {
      var stopWords = stopWordsPerField[fieldName];
      foreach(var text in stopWords) {
        allStopWords.Add(new Term(fieldName, text));
      }
    }
    return allStopWords.ToArray();
	}

}
}
