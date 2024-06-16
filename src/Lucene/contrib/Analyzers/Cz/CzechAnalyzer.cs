/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Collections;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.De;
using Lucene.Net.Analysis.Standard;
using Version = Lucene.Net.Util.Version;

namespace Lucene.Net.Analysis.Cz
{
/*
 * {@link Analyzer} for Czech language. 
 * <p>
 * Supports an external list of stopwords (words that
 * will not be indexed at all). 
 * A default set of stopwords is used unless an alternative list is specified.
 * </p>
 *
 * <p><b>NOTE</b>: This class uses the same {@link Version}
 * dependent settings as {@link StandardAnalyzer}.</p>
 */
public sealed class CzechAnalyzer : Analyzer {

	/*
	 * List of typical stopwords.
	 * @deprecated use {@link #getDefaultStopSet()} instead
	 */
  // TODO make this private in 3.1
	public static readonly String[] CZECH_STOP_WORDS = {
        "a","s","k","o","i","u","v","z","dnes","cz","t\u00edmto","bude\u0161","budem",
        "byli","jse\u0161","m\u016fj","sv\u00fdm","ta","tomto","tohle","tuto","tyto",
        "jej","zda","pro\u010d","m\u00e1te","tato","kam","tohoto","kdo","kte\u0159\u00ed",
        "mi","n\u00e1m","tom","tomuto","m\u00edt","nic","proto","kterou","byla",
        "toho","proto\u017ee","asi","ho","na\u0161i","napi\u0161te","re","co\u017e","t\u00edm",
        "tak\u017ee","sv\u00fdch","jej\u00ed","sv\u00fdmi","jste","aj","tu","tedy","teto",
        "bylo","kde","ke","prav\u00e9","ji","nad","nejsou","\u010di","pod","t\u00e9ma",
        "mezi","p\u0159es","ty","pak","v\u00e1m","ani","kdy\u017e","v\u0161ak","neg","jsem",
        "tento","\u010dl\u00e1nku","\u010dl\u00e1nky","aby","jsme","p\u0159ed","pta","jejich",
        "byl","je\u0161t\u011b","a\u017e","bez","tak\u00e9","pouze","prvn\u00ed","va\u0161e","kter\u00e1",
        "n\u00e1s","nov\u00fd","tipy","pokud","m\u016f\u017ee","strana","jeho","sv\u00e9","jin\u00e9",
        "zpr\u00e1vy","nov\u00e9","nen\u00ed","v\u00e1s","jen","podle","zde","u\u017e","b\u00fdt","v\u00edce",
        "bude","ji\u017e","ne\u017e","kter\u00fd","by","kter\u00e9","co","nebo","ten","tak",
        "m\u00e1","p\u0159i","od","po","jsou","jak","dal\u0161\u00ed","ale","si","se","ve",
        "to","jako","za","zp\u011bt","ze","do","pro","je","na","atd","atp",
        "jakmile","p\u0159i\u010dem\u017e","j\u00e1","on","ona","ono","oni","ony","my","vy",
        "j\u00ed","ji","m\u011b","mne","jemu","tomu","t\u011bm","t\u011bmu","n\u011bmu","n\u011bmu\u017e",
        "jeho\u017e","j\u00ed\u017e","jeliko\u017e","je\u017e","jako\u017e","na\u010de\u017e",
    };
	
	/*
	 * Returns a set of default Czech-stopwords 
	 * @return a set of default Czech-stopwords 
	 */
	public static ISet<string> getDefaultStopSet(){
	  return DefaultSetHolder.DEFAULT_SET;
	}
	
	private static class DefaultSetHolder {
	  internal static ISet<string> DEFAULT_SET = CharArraySet.UnmodifiableSet(new CharArraySet(
          (IEnumerable<string>)CZECH_STOP_WORDS, false));
	}

	/*
	 * Contains the stopwords used with the {@link StopFilter}.
	 */
	// TODO make this final in 3.1
	private ISet<string> stoptable;
  private readonly Version matchVersion;

	/*
	 * Builds an analyzer with the default stop words ({@link #CZECH_STOP_WORDS}).
	 */
	public CzechAnalyzer(Version matchVersion) 
    : this(matchVersion, DefaultSetHolder.DEFAULT_SET)
    {
    
	}
	
	/*
   * Builds an analyzer with the given stop words and stemming exclusion words
   * 
   * @param matchVersion
   *          lucene compatibility version
   * @param stopwords
   *          a stopword set
   */
  public CzechAnalyzer(Version matchVersion, ISet<string> stopwords) {
    this.matchVersion = matchVersion;
    this.stoptable = CharArraySet.UnmodifiableSet(CharArraySet.Copy(stopwords));
  }


	/*
	 * Builds an analyzer with the given stop words.
	 * @deprecated use {@link #CzechAnalyzer(Version, Set)} instead
	 */
  public CzechAnalyzer(Version matchVersion, params string[] stopwords) 
  : this(matchVersion, StopFilter.MakeStopSet( stopwords ))
  {
    
	}

  /*
   * Builds an analyzer with the given stop words.
   * 
   * @deprecated use {@link #CzechAnalyzer(Version, Set)} instead
   */
  public CzechAnalyzer(Version matchVersion, HashSet<string> stopwords) 
  : this(matchVersion, (ISet<string>)stopwords)
  {
    
	}

	/*
	 * Builds an analyzer with the given stop words.
	 * @deprecated use {@link #CzechAnalyzer(Version, Set)} instead
	 */
  public CzechAnalyzer(Version matchVersion, FileInfo stopwords ) 
      : this(matchVersion, WordlistLoader.GetWordSet( stopwords ))
  {
    
	}

    /*
     * Loads stopwords hash from resource stream (file, database...).
     * @param   wordfile    File containing the wordlist
     * @param   encoding    Encoding used (win-1250, iso-8859-2, ...), null for default system encoding
     * @deprecated use {@link WordlistLoader#getWordSet(Reader, String) }
     *             and {@link #CzechAnalyzer(Version, Set)} instead
     */
    public void LoadStopWords( Stream wordfile, System.Text.Encoding encoding ) {
        PreviousTokenStream = null; // force a new stopfilter to be created
        if ( wordfile == null )
        {
            stoptable = Support.Compatibility.SetFactory.CreateHashSet<string>();
            return;
        }
        try {
            // clear any previous table (if present)
            stoptable = Support.Compatibility.SetFactory.CreateHashSet<string>();

            StreamReader isr;
            if (encoding == null)
                isr = new StreamReader(wordfile);
            else
                isr = new StreamReader(wordfile, encoding);

            stoptable = WordlistLoader.GetWordSet(isr);
        } catch ( IOException) {
          // clear any previous table (if present)
          // TODO: throw IOException
            stoptable = Support.Compatibility.SetFactory.CreateHashSet<string>();
        }
    }

	/*
	 * Creates a {@link TokenStream} which tokenizes all the text in the provided {@link Reader}.
	 *
	 * @return  A {@link TokenStream} built from a {@link StandardTokenizer} filtered with
	 * 			{@link StandardFilter}, {@link LowerCaseFilter}, and {@link StopFilter}
	 */
	public override sealed TokenStream TokenStream( String fieldName, TextReader reader ) {
                TokenStream result = new StandardTokenizer( matchVersion, reader );
		result = new StandardFilter( result );
		result = new LowerCaseFilter( result );
		result = new StopFilter( StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                         result, stoptable );
		return result;
	}
	
	private class SavedStreams {
	    protected internal Tokenizer source;
	    protected internal TokenStream result;
	};
	
	/*
     * Returns a (possibly reused) {@link TokenStream} which tokenizes all the text in 
     * the provided {@link Reader}.
     *
     * @return  A {@link TokenStream} built from a {@link StandardTokenizer} filtered with
     *          {@link StandardFilter}, {@link LowerCaseFilter}, and {@link StopFilter}
     */
	public override TokenStream ReusableTokenStream(String fieldName, TextReader reader)
    {
      SavedStreams streams = (SavedStreams) PreviousTokenStream;
      if (streams == null) {
        streams = new SavedStreams();
        streams.source = new StandardTokenizer(matchVersion, reader);
        streams.result = new StandardFilter(streams.source);
        streams.result = new LowerCaseFilter(streams.result);
        streams.result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                        streams.result, stoptable);
        PreviousTokenStream = streams;
      } else {
        streams.source.Reset(reader);
      }
      return streams.result;
    }
}


}
