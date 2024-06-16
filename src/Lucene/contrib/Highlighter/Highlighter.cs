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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Util;

namespace Lucene.Net.Search.Highlight
{
    /// <summary>
    /// Class used to markup highlighted terms found in the best sections of a
    /// text, using configurable <see cref="IFragmenter"/>, <see cref="Scorer"/>, <see cref="IFormatter"/>,
    /// <see cref="IEncoder"/> and tokenizers.
    /// </summary>
    public class Highlighter
    {
        public static readonly int DEFAULT_MAX_CHARS_TO_ANALYZE = 50*1024;

        private int _maxDocCharsToAnalyze = DEFAULT_MAX_CHARS_TO_ANALYZE;
        private IFormatter _formatter;
        private IEncoder _encoder;
        private IFragmenter _textFragmenter = new SimpleFragmenter();
        private IScorer _fragmentScorer = null;

        public Highlighter(IScorer fragmentScorer)
            : this(new SimpleHTMLFormatter(), fragmentScorer)
        {
        }


        public Highlighter(IFormatter formatter, IScorer fragmentScorer)
            : this(formatter, new DefaultEncoder(), fragmentScorer)
        {
        }


        public Highlighter(IFormatter formatter, IEncoder encoder, IScorer fragmentScorer)
        {
            _formatter = formatter;
            _encoder = encoder;
            _fragmentScorer = fragmentScorer;
        }

        /// <summary>
        /// Highlights chosen terms in a text, extracting the most relevant section.
        /// This is a convenience method that calls <see cref="GetBestFragment(TokenStream, string)"/>
        /// </summary>
        /// <param name="analyzer">the analyzer that will be used to split <c>text</c> into chunks</param>
        /// <param name="fieldName">Name of field used to influence analyzer's tokenization policy</param>
        /// <param name="text">text to highlight terms in</param>
        /// <returns>highlighted text fragment or null if no terms found</returns>
        /// <exception cref="InvalidTokenOffsetsException">thrown if any token's endOffset exceeds the provided text's length</exception>
        public String GetBestFragment(Analyzer analyzer, String fieldName, String text)
        {
            TokenStream tokenStream = analyzer.TokenStream(fieldName, new StringReader(text));
            return GetBestFragment(tokenStream, text);
        }

        /// <summary>
        /// Highlights chosen terms in a text, extracting the most relevant section.
        /// The document text is analysed in chunks to record hit statistics
        /// across the document. After accumulating stats, the fragment with the highest score
        /// is returned
        /// </summary>
        /// <param name="tokenStream">
        /// a stream of tokens identified in the text parameter, including offset information.
        /// This is typically produced by an analyzer re-parsing a document's
        /// text. Some work may be done on retrieving TokenStreams more efficiently
        /// by adding support for storing original text position data in the Lucene
        /// index but this support is not currently available (as of Lucene 1.4 rc2).
        /// </param>
        /// <param name="text">text to highlight terms in</param>
        /// <returns>highlighted text fragment or null if no terms found</returns>
        /// <exception cref="InvalidTokenOffsetsException">thrown if any token's endOffset exceeds the provided text's length</exception>
        public String GetBestFragment(TokenStream tokenStream, String text)
        {
            String[] results = GetBestFragments(tokenStream, text, 1);
            if (results.Length > 0)
            {
                return results[0];
            }
            return null;
        }

        /// <summary>
        /// Highlights chosen terms in a text, extracting the most relevant sections.
        /// This is a convenience method that calls <see cref="GetBestFragments(TokenStream, string, int)"/>
        /// </summary>
        /// <param name="analyzer">the analyzer that will be used to split <c>text</c> into chunks</param>
        /// <param name="fieldName">the name of the field being highlighted (used by analyzer)</param>
        /// <param name="text">text to highlight terms in</param>
        /// <param name="maxNumFragments">the maximum number of fragments.</param>
        /// <returns>highlighted text fragments (between 0 and maxNumFragments number of fragments)</returns>
        /// <exception cref="InvalidTokenOffsetsException">thrown if any token's endOffset exceeds the provided text's length</exception>
        public String[] GetBestFragments(
            Analyzer analyzer,
            String fieldName,
            String text,
            int maxNumFragments)
        {
            TokenStream tokenStream = analyzer.TokenStream(fieldName, new StringReader(text));
            return GetBestFragments(tokenStream, text, maxNumFragments);
        }

        /// <summary>
        /// Highlights chosen terms in a text, extracting the most relevant sections.
        /// The document text is analysed in chunks to record hit statistics
        /// across the document. After accumulating stats, the fragments with the highest scores
        /// are returned as an array of strings in order of score (contiguous fragments are merged into
        /// one in their original order to improve readability)
        /// </summary>
        /// <param name="tokenStream"></param>
        /// <param name="text">text to highlight terms in</param>
        /// <param name="maxNumFragments">the maximum number of fragments.</param>
        /// <returns>highlighted text fragments (between 0 and maxNumFragments number of fragments)</returns>
        /// <exception cref="InvalidTokenOffsetsException">thrown if any token's endOffset exceeds the provided text's length</exception>
        public String[] GetBestFragments(TokenStream tokenStream, String text, int maxNumFragments)
        {
            maxNumFragments = Math.Max(1, maxNumFragments); //sanity check

            TextFragment[] frag = GetBestTextFragments(tokenStream, text, true, maxNumFragments);

            //Get text
            var fragTexts = new List<String>();
            for (int i = 0; i < frag.Length; i++)
            {
                if ((frag[i] != null) && (frag[i].Score > 0))
                {
                    fragTexts.Add(frag[i].ToString());
                }
            }
            return fragTexts.ToArray();
        }

        /// <summary>
        /// Low level api to get the most relevant (formatted) sections of the document.
        /// This method has been made public to allow visibility of score information held in TextFragment objects.
        /// Thanks to Jason Calabrese for help in redefining the interface.
        /// </summary>
        public TextFragment[] GetBestTextFragments(
            TokenStream tokenStream,
            String text,
            bool mergeContiguousFragments,
            int maxNumFragments)
        {
            var docFrags = new List<TextFragment>();
            var newText = new StringBuilder();

            var termAtt = tokenStream.AddAttribute<ITermAttribute>();
            var offsetAtt = tokenStream.AddAttribute<IOffsetAttribute>();
            tokenStream.AddAttribute<IPositionIncrementAttribute>();
            tokenStream.Reset();

            var currentFrag = new TextFragment(newText, newText.Length, docFrags.Count);
            var newStream = _fragmentScorer.Init(tokenStream);
            if (newStream != null)
            {
                tokenStream = newStream;
            }
            _fragmentScorer.StartFragment(currentFrag);
            docFrags.Add(currentFrag);

            var fragQueue = new FragmentQueue(maxNumFragments);

            try
            {

                String tokenText;
                int startOffset;
                int endOffset;
                int lastEndOffset = 0;
                _textFragmenter.Start(text, tokenStream);

                var tokenGroup = new TokenGroup(tokenStream);

                for (bool next = tokenStream.IncrementToken();
                     next && (offsetAtt.StartOffset < _maxDocCharsToAnalyze);
                     next = tokenStream.IncrementToken())
                {
                    if ((offsetAtt.EndOffset > text.Length)
                        ||
                        (offsetAtt.StartOffset > text.Length)
                        )
                    {
                        throw new InvalidTokenOffsetsException("Token " + termAtt.Term
                                                               + " exceeds length of provided text sized " + text.Length);
                    }
                    if ((tokenGroup.NumTokens > 0) && (tokenGroup.IsDistinct()))
                    {
                        //the current token is distinct from previous tokens -
                        // markup the cached token group info
                        startOffset = tokenGroup.MatchStartOffset;
                        endOffset = tokenGroup.MatchEndOffset;
                        tokenText = text.Substring(startOffset, endOffset - startOffset);
                        String markedUpText = _formatter.HighlightTerm(_encoder.EncodeText(tokenText), tokenGroup);
                        //store any whitespace etc from between this and last group
                        if (startOffset > lastEndOffset)
                            newText.Append(_encoder.EncodeText(text.Substring(lastEndOffset, startOffset - lastEndOffset)));
                        newText.Append(markedUpText);
                        lastEndOffset = Math.Max(endOffset, lastEndOffset);
                        tokenGroup.Clear();

                        //check if current token marks the start of a new fragment
                        if (_textFragmenter.IsNewFragment())
                        {
                            currentFrag.Score = _fragmentScorer.FragmentScore;
                            //record stats for a new fragment
                            currentFrag.TextEndPos = newText.Length;
                            currentFrag = new TextFragment(newText, newText.Length, docFrags.Count);
                            _fragmentScorer.StartFragment(currentFrag);
                            docFrags.Add(currentFrag);
                        }
                    }

                    tokenGroup.AddToken(_fragmentScorer.GetTokenScore());

                    //				if(lastEndOffset>maxDocBytesToAnalyze)
                    //				{
                    //					break;
                    //				}
                }
                currentFrag.Score = _fragmentScorer.FragmentScore;

                if (tokenGroup.NumTokens > 0)
                {
                    //flush the accumulated text (same code as in above loop)
                    startOffset = tokenGroup.MatchStartOffset;
                    endOffset = tokenGroup.MatchEndOffset;
                    tokenText = text.Substring(startOffset, endOffset - startOffset);
                    var markedUpText = _formatter.HighlightTerm(_encoder.EncodeText(tokenText), tokenGroup);
                    //store any whitespace etc from between this and last group
                    if (startOffset > lastEndOffset)
                        newText.Append(_encoder.EncodeText(text.Substring(lastEndOffset, startOffset - lastEndOffset)));
                    newText.Append(markedUpText);
                    lastEndOffset = Math.Max(lastEndOffset, endOffset);
                }

                //Test what remains of the original text beyond the point where we stopped analyzing 
                if (
                    //					if there is text beyond the last token considered..
                    (lastEndOffset < text.Length)
                    &&
                    //					and that text is not too large...
                    (text.Length <= _maxDocCharsToAnalyze)
                    )
                {
                    //append it to the last fragment
                    newText.Append(_encoder.EncodeText(text.Substring(lastEndOffset)));
                }

                currentFrag.TextEndPos = newText.Length;

                //sort the most relevant sections of the text
                foreach (var f in docFrags)
                {
                    currentFrag = f;

                    //If you are running with a version of Lucene before 11th Sept 03
                    // you do not have PriorityQueue.insert() - so uncomment the code below
                    /*
                                        if (currentFrag.getScore() >= minScore)
                                        {
                                            fragQueue.put(currentFrag);
                                            if (fragQueue.size() > maxNumFragments)
                                            { // if hit queue overfull
                                                fragQueue.pop(); // remove lowest in hit queue
                                                minScore = ((TextFragment) fragQueue.top()).getScore(); // reset minScore
                                            }


                                        }
                    */
                    //The above code caused a problem as a result of Christoph Goller's 11th Sept 03
                    //fix to PriorityQueue. The correct method to use here is the new "insert" method
                    // USE ABOVE CODE IF THIS DOES NOT COMPILE!
                    fragQueue.InsertWithOverflow(currentFrag);
                }

                //return the most relevant fragments
                var frag = new TextFragment[fragQueue.Size()];
                for (int i = frag.Length - 1; i >= 0; i--)
                {
                    frag[i] = fragQueue.Pop();
                }

                //merge any contiguous fragments to improve readability
                if (mergeContiguousFragments)
                {
                    MergeContiguousFragments(frag);
                    frag = frag.Where(t => (t != null) && (t.Score > 0)).ToArray();
                }

                return frag;

            }
            finally
            {
                if (tokenStream != null)
                {
                    try
                    {
                        tokenStream.Close();
                    }
                    catch (Exception)
                    {
                    }
                }
            }
        }

        /// <summary>
        /// Improves readability of a score-sorted list of TextFragments by merging any fragments
        /// that were contiguous in the original text into one larger fragment with the correct order.
        /// This will leave a "null" in the array entry for the lesser scored fragment. 
        /// </summary>
        /// <param name="frag">An array of document fragments in descending score</param>
        private void MergeContiguousFragments(TextFragment[] frag)
        {
            bool mergingStillBeingDone;
            if (frag.Length > 1)
                do
                {
                    mergingStillBeingDone = false; //initialise loop control flag
                    //for each fragment, scan other frags looking for contiguous blocks
                    for (int i = 0; i < frag.Length; i++)
                    {
                        if (frag[i] == null)
                        {
                            continue;
                        }
                        //merge any contiguous blocks 
                        for (int x = 0; x < frag.Length; x++)
                        {
                            if (frag[x] == null)
                            {
                                continue;
                            }
                            if (frag[i] == null)
                            {
                                break;
                            }
                            TextFragment frag1 = null;
                            TextFragment frag2 = null;
                            int frag1Num = 0;
                            int frag2Num = 0;
                            int bestScoringFragNum;
                            int worstScoringFragNum;
                            //if blocks are contiguous....
                            if (frag[i].Follows(frag[x]))
                            {
                                frag1 = frag[x];
                                frag1Num = x;
                                frag2 = frag[i];
                                frag2Num = i;
                            }
                            else if (frag[x].Follows(frag[i]))
                            {
                                frag1 = frag[i];
                                frag1Num = i;
                                frag2 = frag[x];
                                frag2Num = x;
                            }
                            //merging required..
                            if (frag1 != null)
                            {
                                if (frag1.Score > frag2.Score)
                                {
                                    bestScoringFragNum = frag1Num;
                                    worstScoringFragNum = frag2Num;
                                }
                                else
                                {
                                    bestScoringFragNum = frag2Num;
                                    worstScoringFragNum = frag1Num;
                                }
                                frag1.Merge(frag2);
                                frag[worstScoringFragNum] = null;
                                mergingStillBeingDone = true;
                                frag[bestScoringFragNum] = frag1;
                            }
                        }
                    }
                } while (mergingStillBeingDone);
        }

        /// <summary>
        /// Highlights terms in the  text , extracting the most relevant sections
        /// and concatenating the chosen fragments with a separator (typically "...").
        /// The document text is analysed in chunks to record hit statistics
        /// across the document. After accumulating stats, the fragments with the highest scores
        /// are returned in order as "separator" delimited strings.
        /// </summary>
        /// <param name="tokenStream"></param>
        /// <param name="text">text to highlight terms in</param>
        /// <param name="maxNumFragments">the maximum number of fragments.</param>
        /// <param name="separator">the separator used to intersperse the document fragments (typically "...")</param>
        /// <returns>highlighted text</returns>
        public String GetBestFragments(
            TokenStream tokenStream,
            String text,
            int maxNumFragments,
            String separator)
        {
            string[] sections = GetBestFragments(tokenStream, text, maxNumFragments);
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < sections.Length; i++)
            {
                if (i > 0)
                {
                    result.Append(separator);
                }
                result.Append(sections[i]);
            }
            return result.ToString();
        }

        public int MaxDocCharsToAnalyze
        {
            get { return _maxDocCharsToAnalyze; }
            set { this._maxDocCharsToAnalyze = value; }
        }


        public IFragmenter TextFragmenter
        {
            get { return _textFragmenter; }
            set { _textFragmenter = value; }
        }

        public IScorer FragmentScorer
        {
            get { return _fragmentScorer; }
            set { _fragmentScorer = value; }
        }

        public IEncoder Encoder
        {
            get { return _encoder; }
            set { this._encoder = value; }
        }
    }

    internal class FragmentQueue : PriorityQueue<TextFragment>
    {
        public FragmentQueue(int size)
        {
            Initialize(size);
        }

        public override bool LessThan(TextFragment fragA, TextFragment fragB)
        {
            if (fragA.Score == fragB.Score)
                return fragA.FragNum > fragB.FragNum;
            else
                return fragA.Score < fragB.Score;
        }
    }
}
