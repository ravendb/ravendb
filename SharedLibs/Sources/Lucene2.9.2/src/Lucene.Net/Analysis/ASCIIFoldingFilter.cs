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

using TermAttribute = Lucene.Net.Analysis.Tokenattributes.TermAttribute;
using ArrayUtil = Lucene.Net.Util.ArrayUtil;

namespace Lucene.Net.Analysis
{
	
	/// <summary> This class converts alphabetic, numeric, and symbolic Unicode characters
	/// which are not in the first 127 ASCII characters (the "Basic Latin" Unicode
	/// block) into their ASCII equivalents, if one exists.
	/// 
	/// Characters from the following Unicode blocks are converted; however, only
	/// those characters with reasonable ASCII alternatives are converted:
	/// 
	/// <ul>
	/// <li>C1 Controls and Latin-1 Supplement: <a href="http://www.unicode.org/charts/PDF/U0080.pdf">http://www.unicode.org/charts/PDF/U0080.pdf</a></li>
    /// <li>Latin Extended-A: <a href="http://www.unicode.org/charts/PDF/U0100.pdf">http://www.unicode.org/charts/PDF/U0100.pdf</a></li>
    /// <li>Latin Extended-B: <a href="http://www.unicode.org/charts/PDF/U0180.pdf">http://www.unicode.org/charts/PDF/U0180.pdf</a></li>
    /// <li>Latin Extended Additional: <a href="http://www.unicode.org/charts/PDF/U1E00.pdf">http://www.unicode.org/charts/PDF/U1E00.pdf</a></li>
    /// <li>Latin Extended-C: <a href="http://www.unicode.org/charts/PDF/U2C60.pdf">http://www.unicode.org/charts/PDF/U2C60.pdf</a></li>
    /// <li>Latin Extended-D: <a href="http://www.unicode.org/charts/PDF/UA720.pdf">http://www.unicode.org/charts/PDF/UA720.pdf</a></li>
    /// <li>IPA Extensions: <a href="http://www.unicode.org/charts/PDF/U0250.pdf">http://www.unicode.org/charts/PDF/U0250.pdf</a></li>
    /// <li>Phonetic Extensions: <a href="http://www.unicode.org/charts/PDF/U1D00.pdf">http://www.unicode.org/charts/PDF/U1D00.pdf</a></li>
    /// <li>Phonetic Extensions Supplement: <a href="http://www.unicode.org/charts/PDF/U1D80.pdf">http://www.unicode.org/charts/PDF/U1D80.pdf</a></li>
    /// <li>General Punctuation: <a href="http://www.unicode.org/charts/PDF/U2000.pdf">http://www.unicode.org/charts/PDF/U2000.pdf</a></li>
    /// <li>Superscripts and Subscripts: <a href="http://www.unicode.org/charts/PDF/U2070.pdf">http://www.unicode.org/charts/PDF/U2070.pdf</a></li>
    /// <li>Enclosed Alphanumerics: <a href="http://www.unicode.org/charts/PDF/U2460.pdf">http://www.unicode.org/charts/PDF/U2460.pdf</a></li>
    /// <li>Dingbats: <a href="http://www.unicode.org/charts/PDF/U2700.pdf">http://www.unicode.org/charts/PDF/U2700.pdf</a></li>
    /// <li>Supplemental Punctuation: <a href="http://www.unicode.org/charts/PDF/U2E00.pdf">http://www.unicode.org/charts/PDF/U2E00.pdf</a></li>
    /// <li>Alphabetic Presentation Forms: <a href="http://www.unicode.org/charts/PDF/UFB00.pdf">http://www.unicode.org/charts/PDF/UFB00.pdf</a></li>
    /// <li>Halfwidth and Fullwidth Forms: <a href="http://www.unicode.org/charts/PDF/UFF00.pdf">http://www.unicode.org/charts/PDF/UFF00.pdf</a></li>
	/// </ul>
	/// 
	/// See: <a href="http://en.wikipedia.org/wiki/Latin_characters_in_Unicode">http://en.wikipedia.org/wiki/Latin_characters_in_Unicode</a>
	/// 
	/// The set of character conversions supported by this class is a superset of
	/// those supported by Lucene's {@link ISOLatin1AccentFilter} which strips
	/// accents from Latin1 characters.  For example, '&#192;' will be replaced by
	/// 'a'.
	/// </summary>
	public sealed class ASCIIFoldingFilter:TokenFilter
	{
		public ASCIIFoldingFilter(TokenStream input):base(input)
		{
			termAtt = (TermAttribute) AddAttribute(typeof(TermAttribute));
		}
		
		private char[] output = new char[512];
		private int outputPos;
		private TermAttribute termAtt;
		
		public override bool IncrementToken()
		{
			if (input.IncrementToken())
			{
				char[] buffer = termAtt.TermBuffer();
				int length = termAtt.TermLength();
				
				// If no characters actually require rewriting then we
				// just return token as-is:
				for (int i = 0; i < length; ++i)
				{
					char c = buffer[i];
					if (c >= '\u0080')
					{
						FoldToASCII(buffer, length);
						termAtt.SetTermBuffer(output, 0, outputPos);
						break;
					}
				}
				return true;
			}
			else
			{
				return false;
			}
		}
		
		/// <summary> Converts characters above ASCII to their ASCII equivalents.  For example,
		/// accents are removed from accented characters.
		/// </summary>
		/// <param name="input">The string to fold
		/// </param>
		/// <param name="length">The number of characters in the input string
		/// </param>
		public void  FoldToASCII(char[] input, int length)
		{
			// Worst-case length required:
			int maxSizeNeeded = 4 * length;
			if (output.Length < maxSizeNeeded)
			{
				output = new char[ArrayUtil.GetNextSize(maxSizeNeeded)];
			}
			
			outputPos = 0;
			
			for (int pos = 0; pos < length; ++pos)
			{
				char c = input[pos];
				
				// Quick test: if it's not in range then just keep current character
				if (c < '\u0080')
				{
					output[outputPos++] = c;
				}
				else
				{
					switch (c)
					{
						
						case '\u00C0': 
						// Ã€  [LATIN CAPITAL LETTER A WITH GRAVE]
						case '\u00C1': 
						// Ã�  [LATIN CAPITAL LETTER A WITH ACUTE]
						case '\u00C2': 
						// Ã‚  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX]
						case '\u00C3': 
						// Ãƒ  [LATIN CAPITAL LETTER A WITH TILDE]
						case '\u00C4': 
						// Ã„  [LATIN CAPITAL LETTER A WITH DIAERESIS]
						case '\u00C5': 
						// Ã…  [LATIN CAPITAL LETTER A WITH RING ABOVE]
						case '\u0100': 
						// Ä€  [LATIN CAPITAL LETTER A WITH MACRON]
						case '\u0102': 
						// Ä‚  [LATIN CAPITAL LETTER A WITH BREVE]
						case '\u0104': 
						// Ä„  [LATIN CAPITAL LETTER A WITH OGONEK]
						case '\u018F': 
						// Æ�  http://en.wikipedia.org/wiki/Schwa  [LATIN CAPITAL LETTER SCHWA]
						case '\u01CD': 
						// Ç�  [LATIN CAPITAL LETTER A WITH CARON]
						case '\u01DE': 
						// Çž  [LATIN CAPITAL LETTER A WITH DIAERESIS AND MACRON]
						case '\u01E0': 
						// Ç   [LATIN CAPITAL LETTER A WITH DOT ABOVE AND MACRON]
						case '\u01FA': 
						// Çº  [LATIN CAPITAL LETTER A WITH RING ABOVE AND ACUTE]
						case '\u0200': 
						// È€  [LATIN CAPITAL LETTER A WITH DOUBLE GRAVE]
						case '\u0202': 
						// È‚  [LATIN CAPITAL LETTER A WITH INVERTED BREVE]
						case '\u0226': 
						// È¦  [LATIN CAPITAL LETTER A WITH DOT ABOVE]
						case '\u023A': 
						// Èº  [LATIN CAPITAL LETTER A WITH STROKE]
						case '\u1D00': 
						// á´€  [LATIN LETTER SMALL CAPITAL A]
						case '\u1E00': 
						// á¸€  [LATIN CAPITAL LETTER A WITH RING BELOW]
						case '\u1EA0': 
						// áº   [LATIN CAPITAL LETTER A WITH DOT BELOW]
						case '\u1EA2': 
						// áº¢  [LATIN CAPITAL LETTER A WITH HOOK ABOVE]
						case '\u1EA4': 
						// áº¤  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND ACUTE]
						case '\u1EA6': 
						// áº¦  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND GRAVE]
						case '\u1EA8': 
						// áº¨  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE]
						case '\u1EAA': 
						// áºª  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND TILDE]
						case '\u1EAC': 
						// áº¬  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND DOT BELOW]
						case '\u1EAE': 
						// áº®  [LATIN CAPITAL LETTER A WITH BREVE AND ACUTE]
						case '\u1EB0': 
						// áº°  [LATIN CAPITAL LETTER A WITH BREVE AND GRAVE]
						case '\u1EB2': 
						// áº²  [LATIN CAPITAL LETTER A WITH BREVE AND HOOK ABOVE]
						case '\u1EB4': 
						// áº´  [LATIN CAPITAL LETTER A WITH BREVE AND TILDE]
						case '\u1EB6': 
						// áº¶  [LATIN CAPITAL LETTER A WITH BREVE AND DOT BELOW]
						case '\u24B6': 
						// â’¶  [CIRCLED LATIN CAPITAL LETTER A]
						case '\uFF21':  // ï¼¡  [FULLWIDTH LATIN CAPITAL LETTER A]
							output[outputPos++] = 'A';
							break;
						
						case '\u00E0': 
						// Ã   [LATIN SMALL LETTER A WITH GRAVE]
						case '\u00E1': 
						// Ã¡  [LATIN SMALL LETTER A WITH ACUTE]
						case '\u00E2': 
						// Ã¢  [LATIN SMALL LETTER A WITH CIRCUMFLEX]
						case '\u00E3': 
						// Ã£  [LATIN SMALL LETTER A WITH TILDE]
						case '\u00E4': 
						// Ã¤  [LATIN SMALL LETTER A WITH DIAERESIS]
						case '\u00E5': 
						// Ã¥  [LATIN SMALL LETTER A WITH RING ABOVE]
						case '\u0101': 
						// Ä�  [LATIN SMALL LETTER A WITH MACRON]
						case '\u0103': 
						// Äƒ  [LATIN SMALL LETTER A WITH BREVE]
						case '\u0105': 
						// Ä…  [LATIN SMALL LETTER A WITH OGONEK]
						case '\u01CE': 
						// ÇŽ  [LATIN SMALL LETTER A WITH CARON]
						case '\u01DF': 
						// ÇŸ  [LATIN SMALL LETTER A WITH DIAERESIS AND MACRON]
						case '\u01E1': 
						// Ç¡  [LATIN SMALL LETTER A WITH DOT ABOVE AND MACRON]
						case '\u01FB': 
						// Ç»  [LATIN SMALL LETTER A WITH RING ABOVE AND ACUTE]
						case '\u0201': 
						// È�  [LATIN SMALL LETTER A WITH DOUBLE GRAVE]
						case '\u0203': 
						// Èƒ  [LATIN SMALL LETTER A WITH INVERTED BREVE]
						case '\u0227': 
						// È§  [LATIN SMALL LETTER A WITH DOT ABOVE]
						case '\u0250': 
						// É�  [LATIN SMALL LETTER TURNED A]
						case '\u0259': 
						// É™  [LATIN SMALL LETTER SCHWA]
						case '\u025A': 
						// Éš  [LATIN SMALL LETTER SCHWA WITH HOOK]
						case '\u1D8F': 
						// á¶�  [LATIN SMALL LETTER A WITH RETROFLEX HOOK]
						case '\u1D95': 
						// á¶•  [LATIN SMALL LETTER SCHWA WITH RETROFLEX HOOK]
						case '\u1E01': 
						// áº¡  [LATIN SMALL LETTER A WITH RING BELOW]
						case '\u1E9A': 
						// áº£  [LATIN SMALL LETTER A WITH RIGHT HALF RING]
						case '\u1EA1': 
						// áº¡  [LATIN SMALL LETTER A WITH DOT BELOW]
						case '\u1EA3': 
						// áº£  [LATIN SMALL LETTER A WITH HOOK ABOVE]
						case '\u1EA5': 
						// áº¥  [LATIN SMALL LETTER A WITH CIRCUMFLEX AND ACUTE]
						case '\u1EA7': 
						// áº§  [LATIN SMALL LETTER A WITH CIRCUMFLEX AND GRAVE]
						case '\u1EA9': 
						// áº©  [LATIN SMALL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE]
						case '\u1EAB': 
						// áº«  [LATIN SMALL LETTER A WITH CIRCUMFLEX AND TILDE]
						case '\u1EAD': 
						// áº­  [LATIN SMALL LETTER A WITH CIRCUMFLEX AND DOT BELOW]
						case '\u1EAF': 
						// áº¯  [LATIN SMALL LETTER A WITH BREVE AND ACUTE]
						case '\u1EB1': 
						// áº±  [LATIN SMALL LETTER A WITH BREVE AND GRAVE]
						case '\u1EB3': 
						// áº³  [LATIN SMALL LETTER A WITH BREVE AND HOOK ABOVE]
						case '\u1EB5': 
						// áºµ  [LATIN SMALL LETTER A WITH BREVE AND TILDE]
						case '\u1EB7': 
						// áº·  [LATIN SMALL LETTER A WITH BREVE AND DOT BELOW]
						case '\u2090': 
						// â‚�  [LATIN SUBSCRIPT SMALL LETTER A]
						case '\u2094': 
						// â‚�?  [LATIN SUBSCRIPT SMALL LETTER SCHWA]
						case '\u24D0': 
						// â“�  [CIRCLED LATIN SMALL LETTER A]
						case '\u2C65': 
						// â±¥  [LATIN SMALL LETTER A WITH STROKE]
						case '\u2C6F': 
						// â±¯  [LATIN CAPITAL LETTER TURNED A]
						case '\uFF41':  // ï½�  [FULLWIDTH LATIN SMALL LETTER A]
							output[outputPos++] = 'a';
							break;
						
						case '\uA732':  // êœ²  [LATIN CAPITAL LETTER AA]
							output[outputPos++] = 'A';
							output[outputPos++] = 'A';
							break;
						
						case '\u00C6': 
						// Ã†  [LATIN CAPITAL LETTER AE]
						case '\u01E2': 
						// Ç¢  [LATIN CAPITAL LETTER AE WITH MACRON]
						case '\u01FC': 
						// Ç¼  [LATIN CAPITAL LETTER AE WITH ACUTE]
						case '\u1D01':  // á´�  [LATIN LETTER SMALL CAPITAL AE]
							output[outputPos++] = 'A';
							output[outputPos++] = 'E';
							break;
						
						case '\uA734':  // êœ´  [LATIN CAPITAL LETTER AO]
							output[outputPos++] = 'A';
							output[outputPos++] = 'O';
							break;
						
						case '\uA736':  // êœ¶  [LATIN CAPITAL LETTER AU]
							output[outputPos++] = 'A';
							output[outputPos++] = 'U';
							break;
						
						case '\uA738': 
						// êœ¸  [LATIN CAPITAL LETTER AV]
						case '\uA73A':  // êœº  [LATIN CAPITAL LETTER AV WITH HORIZONTAL BAR]
							output[outputPos++] = 'A';
							output[outputPos++] = 'V';
							break;
						
						case '\uA73C':  // êœ¼  [LATIN CAPITAL LETTER AY]
							output[outputPos++] = 'A';
							output[outputPos++] = 'Y';
							break;
						
						case '\u249C':  // â’œ  [PARENTHESIZED LATIN SMALL LETTER A]
							output[outputPos++] = '(';
							output[outputPos++] = 'a';
							output[outputPos++] = ')';
							break;
						
						case '\uA733':  // êœ³  [LATIN SMALL LETTER AA]
							output[outputPos++] = 'a';
							output[outputPos++] = 'a';
							break;
						
						case '\u00E6': 
						// Ã¦  [LATIN SMALL LETTER AE]
						case '\u01E3': 
						// Ç£  [LATIN SMALL LETTER AE WITH MACRON]
						case '\u01FD': 
						// Ç½  [LATIN SMALL LETTER AE WITH ACUTE]
						case '\u1D02':  // á´‚  [LATIN SMALL LETTER TURNED AE]
							output[outputPos++] = 'a';
							output[outputPos++] = 'e';
							break;
						
						case '\uA735':  // êœµ  [LATIN SMALL LETTER AO]
							output[outputPos++] = 'a';
							output[outputPos++] = 'o';
							break;
						
						case '\uA737':  // êœ·  [LATIN SMALL LETTER AU]
							output[outputPos++] = 'a';
							output[outputPos++] = 'u';
							break;
						
						case '\uA739': 
						// êœ¹  [LATIN SMALL LETTER AV]
						case '\uA73B':  // êœ»  [LATIN SMALL LETTER AV WITH HORIZONTAL BAR]
							output[outputPos++] = 'a';
							output[outputPos++] = 'v';
							break;
						
						case '\uA73D':  // êœ½  [LATIN SMALL LETTER AY]
							output[outputPos++] = 'a';
							output[outputPos++] = 'y';
							break;
						
						case '\u0181': 
						// Æ�  [LATIN CAPITAL LETTER B WITH HOOK]
						case '\u0182': 
						// Æ‚  [LATIN CAPITAL LETTER B WITH TOPBAR]
						case '\u0243': 
						// Éƒ  [LATIN CAPITAL LETTER B WITH STROKE]
						case '\u0299': 
						// Ê™  [LATIN LETTER SMALL CAPITAL B]
						case '\u1D03': 
						// á´ƒ  [LATIN LETTER SMALL CAPITAL BARRED B]
						case '\u1E02': 
						// á¸‚  [LATIN CAPITAL LETTER B WITH DOT ABOVE]
						case '\u1E04': 
						// á¸„  [LATIN CAPITAL LETTER B WITH DOT BELOW]
						case '\u1E06': 
						// á¸†  [LATIN CAPITAL LETTER B WITH LINE BELOW]
						case '\u24B7': 
						// â’·  [CIRCLED LATIN CAPITAL LETTER B]
						case '\uFF22':  // ï¼¢  [FULLWIDTH LATIN CAPITAL LETTER B]
							output[outputPos++] = 'B';
							break;
						
						case '\u0180': 
						// Æ€  [LATIN SMALL LETTER B WITH STROKE]
						case '\u0183': 
						// Æƒ  [LATIN SMALL LETTER B WITH TOPBAR]
						case '\u0253': 
						// É“  [LATIN SMALL LETTER B WITH HOOK]
						case '\u1D6C': 
						// áµ¬  [LATIN SMALL LETTER B WITH MIDDLE TILDE]
						case '\u1D80': 
						// á¶€  [LATIN SMALL LETTER B WITH PALATAL HOOK]
						case '\u1E03': 
						// á¸ƒ  [LATIN SMALL LETTER B WITH DOT ABOVE]
						case '\u1E05': 
						// á¸…  [LATIN SMALL LETTER B WITH DOT BELOW]
						case '\u1E07': 
						// á¸‡  [LATIN SMALL LETTER B WITH LINE BELOW]
						case '\u24D1': 
						// â“‘  [CIRCLED LATIN SMALL LETTER B]
						case '\uFF42':  // ï½‚  [FULLWIDTH LATIN SMALL LETTER B]
							output[outputPos++] = 'b';
							break;
						
						case '\u249D':  // â’�  [PARENTHESIZED LATIN SMALL LETTER B]
							output[outputPos++] = '(';
							output[outputPos++] = 'b';
							output[outputPos++] = ')';
							break;
						
						case '\u00C7': 
						// Ã‡  [LATIN CAPITAL LETTER C WITH CEDILLA]
						case '\u0106': 
						// Ä†  [LATIN CAPITAL LETTER C WITH ACUTE]
						case '\u0108': 
						// Äˆ  [LATIN CAPITAL LETTER C WITH CIRCUMFLEX]
						case '\u010A': 
						// ÄŠ  [LATIN CAPITAL LETTER C WITH DOT ABOVE]
						case '\u010C': 
						// ÄŒ  [LATIN CAPITAL LETTER C WITH CARON]
						case '\u0187': 
						// Æ‡  [LATIN CAPITAL LETTER C WITH HOOK]
						case '\u023B': 
						// È»  [LATIN CAPITAL LETTER C WITH STROKE]
						case '\u0297': 
						// Ê—  [LATIN LETTER STRETCHED C]
						case '\u1D04': 
						// á´„  [LATIN LETTER SMALL CAPITAL C]
						case '\u1E08': 
						// á¸ˆ  [LATIN CAPITAL LETTER C WITH CEDILLA AND ACUTE]
						case '\u24B8': 
						// â’¸  [CIRCLED LATIN CAPITAL LETTER C]
						case '\uFF23':  // ï¼£  [FULLWIDTH LATIN CAPITAL LETTER C]
							output[outputPos++] = 'C';
							break;
						
						case '\u00E7': 
						// Ã§  [LATIN SMALL LETTER C WITH CEDILLA]
						case '\u0107': 
						// Ä‡  [LATIN SMALL LETTER C WITH ACUTE]
						case '\u0109': 
						// Ä‰  [LATIN SMALL LETTER C WITH CIRCUMFLEX]
						case '\u010B': 
						// Ä‹  [LATIN SMALL LETTER C WITH DOT ABOVE]
						case '\u010D': 
						// Ä�  [LATIN SMALL LETTER C WITH CARON]
						case '\u0188': 
						// Æˆ  [LATIN SMALL LETTER C WITH HOOK]
						case '\u023C': 
						// È¼  [LATIN SMALL LETTER C WITH STROKE]
						case '\u0255': 
						// É•  [LATIN SMALL LETTER C WITH CURL]
						case '\u1E09': 
						// á¸‰  [LATIN SMALL LETTER C WITH CEDILLA AND ACUTE]
						case '\u2184': 
						// â†„  [LATIN SMALL LETTER REVERSED C]
						case '\u24D2': 
						// â“’  [CIRCLED LATIN SMALL LETTER C]
						case '\uA73E': 
						// êœ¾  [LATIN CAPITAL LETTER REVERSED C WITH DOT]
						case '\uA73F': 
						// êœ¿  [LATIN SMALL LETTER REVERSED C WITH DOT]
						case '\uFF43':  // ï½ƒ  [FULLWIDTH LATIN SMALL LETTER C]
							output[outputPos++] = 'c';
							break;
						
						case '\u249E':  // â’ž  [PARENTHESIZED LATIN SMALL LETTER C]
							output[outputPos++] = '(';
							output[outputPos++] = 'c';
							output[outputPos++] = ')';
							break;
						
						case '\u00D0': 
						// Ã�  [LATIN CAPITAL LETTER ETH]
						case '\u010E': 
						// ÄŽ  [LATIN CAPITAL LETTER D WITH CARON]
						case '\u0110': 
						// Ä�  [LATIN CAPITAL LETTER D WITH STROKE]
						case '\u0189': 
						// Æ‰  [LATIN CAPITAL LETTER AFRICAN D]
						case '\u018A': 
						// ÆŠ  [LATIN CAPITAL LETTER D WITH HOOK]
						case '\u018B': 
						// Æ‹  [LATIN CAPITAL LETTER D WITH TOPBAR]
						case '\u1D05': 
						// á´…  [LATIN LETTER SMALL CAPITAL D]
						case '\u1D06': 
						// á´†  [LATIN LETTER SMALL CAPITAL ETH]
						case '\u1E0A': 
						// á¸Š  [LATIN CAPITAL LETTER D WITH DOT ABOVE]
						case '\u1E0C': 
						// á¸Œ  [LATIN CAPITAL LETTER D WITH DOT BELOW]
						case '\u1E0E': 
						// á¸Ž  [LATIN CAPITAL LETTER D WITH LINE BELOW]
						case '\u1E10': 
						// á¸�  [LATIN CAPITAL LETTER D WITH CEDILLA]
						case '\u1E12': 
						// á¸’  [LATIN CAPITAL LETTER D WITH CIRCUMFLEX BELOW]
						case '\u24B9': 
						// â’¹  [CIRCLED LATIN CAPITAL LETTER D]
						case '\uA779': 
						// ê�¹  [LATIN CAPITAL LETTER INSULAR D]
						case '\uFF24':  // ï¼¤  [FULLWIDTH LATIN CAPITAL LETTER D]
							output[outputPos++] = 'D';
							break;
						
						case '\u00F0': 
						// Ã°  [LATIN SMALL LETTER ETH]
						case '\u010F': 
						// Ä�  [LATIN SMALL LETTER D WITH CARON]
						case '\u0111': 
						// Ä‘  [LATIN SMALL LETTER D WITH STROKE]
						case '\u018C': 
						// ÆŒ  [LATIN SMALL LETTER D WITH TOPBAR]
						case '\u0221': 
						// È¡  [LATIN SMALL LETTER D WITH CURL]
						case '\u0256': 
						// É–  [LATIN SMALL LETTER D WITH TAIL]
						case '\u0257': 
						// É—  [LATIN SMALL LETTER D WITH HOOK]
						case '\u1D6D': 
						// áµ­  [LATIN SMALL LETTER D WITH MIDDLE TILDE]
						case '\u1D81': 
						// á¶�  [LATIN SMALL LETTER D WITH PALATAL HOOK]
						case '\u1D91': 
						// á¶‘  [LATIN SMALL LETTER D WITH HOOK AND TAIL]
						case '\u1E0B': 
						// á¸‹  [LATIN SMALL LETTER D WITH DOT ABOVE]
						case '\u1E0D': 
						// á¸�  [LATIN SMALL LETTER D WITH DOT BELOW]
						case '\u1E0F': 
						// á¸�  [LATIN SMALL LETTER D WITH LINE BELOW]
						case '\u1E11': 
						// á¸‘  [LATIN SMALL LETTER D WITH CEDILLA]
						case '\u1E13': 
						// á¸“  [LATIN SMALL LETTER D WITH CIRCUMFLEX BELOW]
						case '\u24D3': 
						// â““  [CIRCLED LATIN SMALL LETTER D]
						case '\uA77A': 
						// ê�º  [LATIN SMALL LETTER INSULAR D]
						case '\uFF44':  // ï½„  [FULLWIDTH LATIN SMALL LETTER D]
							output[outputPos++] = 'd';
							break;
						
						case '\u01C4': 
						// Ç„  [LATIN CAPITAL LETTER DZ WITH CARON]
						case '\u01F1':  // Ç±  [LATIN CAPITAL LETTER DZ]
							output[outputPos++] = 'D';
							output[outputPos++] = 'Z';
							break;
						
						case '\u01C5': 
						// Ç…  [LATIN CAPITAL LETTER D WITH SMALL LETTER Z WITH CARON]
						case '\u01F2':  // Ç²  [LATIN CAPITAL LETTER D WITH SMALL LETTER Z]
							output[outputPos++] = 'D';
							output[outputPos++] = 'z';
							break;
						
						case '\u249F':  // â’Ÿ  [PARENTHESIZED LATIN SMALL LETTER D]
							output[outputPos++] = '(';
							output[outputPos++] = 'd';
							output[outputPos++] = ')';
							break;
						
						case '\u0238':  // È¸  [LATIN SMALL LETTER DB DIGRAPH]
							output[outputPos++] = 'd';
							output[outputPos++] = 'b';
							break;
						
						case '\u01C6': 
						// Ç†  [LATIN SMALL LETTER DZ WITH CARON]
						case '\u01F3': 
						// Ç³  [LATIN SMALL LETTER DZ]
						case '\u02A3': 
						// Ê£  [LATIN SMALL LETTER DZ DIGRAPH]
						case '\u02A5':  // Ê¥  [LATIN SMALL LETTER DZ DIGRAPH WITH CURL]
							output[outputPos++] = 'd';
							output[outputPos++] = 'z';
							break;
						
						case '\u00C8': 
						// Ãˆ  [LATIN CAPITAL LETTER E WITH GRAVE]
						case '\u00C9': 
						// Ã‰  [LATIN CAPITAL LETTER E WITH ACUTE]
						case '\u00CA': 
						// ÃŠ  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX]
						case '\u00CB': 
						// Ã‹  [LATIN CAPITAL LETTER E WITH DIAERESIS]
						case '\u0112': 
						// Ä’  [LATIN CAPITAL LETTER E WITH MACRON]
						case '\u0114': 
						// Ä�?  [LATIN CAPITAL LETTER E WITH BREVE]
						case '\u0116': 
						// Ä–  [LATIN CAPITAL LETTER E WITH DOT ABOVE]
						case '\u0118': 
						// Ä˜  [LATIN CAPITAL LETTER E WITH OGONEK]
						case '\u011A': 
						// Äš  [LATIN CAPITAL LETTER E WITH CARON]
						case '\u018E': 
						// ÆŽ  [LATIN CAPITAL LETTER REVERSED E]
						case '\u0190': 
						// Æ�  [LATIN CAPITAL LETTER OPEN E]
						case '\u0204': 
						// È„  [LATIN CAPITAL LETTER E WITH DOUBLE GRAVE]
						case '\u0206': 
						// È†  [LATIN CAPITAL LETTER E WITH INVERTED BREVE]
						case '\u0228': 
						// È¨  [LATIN CAPITAL LETTER E WITH CEDILLA]
						case '\u0246': 
						// É†  [LATIN CAPITAL LETTER E WITH STROKE]
						case '\u1D07': 
						// á´‡  [LATIN LETTER SMALL CAPITAL E]
						case '\u1E14': 
						// á¸�?  [LATIN CAPITAL LETTER E WITH MACRON AND GRAVE]
						case '\u1E16': 
						// á¸–  [LATIN CAPITAL LETTER E WITH MACRON AND ACUTE]
						case '\u1E18': 
						// á¸˜  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX BELOW]
						case '\u1E1A': 
						// á¸š  [LATIN CAPITAL LETTER E WITH TILDE BELOW]
						case '\u1E1C': 
						// á¸œ  [LATIN CAPITAL LETTER E WITH CEDILLA AND BREVE]
						case '\u1EB8': 
						// áº¸  [LATIN CAPITAL LETTER E WITH DOT BELOW]
						case '\u1EBA': 
						// áºº  [LATIN CAPITAL LETTER E WITH HOOK ABOVE]
						case '\u1EBC': 
						// áº¼  [LATIN CAPITAL LETTER E WITH TILDE]
						case '\u1EBE': 
						// áº¾  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND ACUTE]
						case '\u1EC0': 
						// á»€  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND GRAVE]
						case '\u1EC2': 
						// á»‚  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE]
						case '\u1EC4': 
						// á»„  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND TILDE]
						case '\u1EC6': 
						// á»†  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND DOT BELOW]
						case '\u24BA': 
						// â’º  [CIRCLED LATIN CAPITAL LETTER E]
						case '\u2C7B': 
						// â±»  [LATIN LETTER SMALL CAPITAL TURNED E]
						case '\uFF25':  // ï¼¥  [FULLWIDTH LATIN CAPITAL LETTER E]
							output[outputPos++] = 'E';
							break;
						
						case '\u00E8': 
						// Ã¨  [LATIN SMALL LETTER E WITH GRAVE]
						case '\u00E9': 
						// Ã©  [LATIN SMALL LETTER E WITH ACUTE]
						case '\u00EA': 
						// Ãª  [LATIN SMALL LETTER E WITH CIRCUMFLEX]
						case '\u00EB': 
						// Ã«  [LATIN SMALL LETTER E WITH DIAERESIS]
						case '\u0113': 
						// Ä“  [LATIN SMALL LETTER E WITH MACRON]
						case '\u0115': 
						// Ä•  [LATIN SMALL LETTER E WITH BREVE]
						case '\u0117': 
						// Ä—  [LATIN SMALL LETTER E WITH DOT ABOVE]
						case '\u0119': 
						// Ä™  [LATIN SMALL LETTER E WITH OGONEK]
						case '\u011B': 
						// Ä›  [LATIN SMALL LETTER E WITH CARON]
						case '\u01DD': 
						// Ç�  [LATIN SMALL LETTER TURNED E]
						case '\u0205': 
						// È…  [LATIN SMALL LETTER E WITH DOUBLE GRAVE]
						case '\u0207': 
						// È‡  [LATIN SMALL LETTER E WITH INVERTED BREVE]
						case '\u0229': 
						// È©  [LATIN SMALL LETTER E WITH CEDILLA]
						case '\u0247': 
						// É‡  [LATIN SMALL LETTER E WITH STROKE]
						case '\u0258': 
						// É˜  [LATIN SMALL LETTER REVERSED E]
						case '\u025B': 
						// É›  [LATIN SMALL LETTER OPEN E]
						case '\u025C': 
						// Éœ  [LATIN SMALL LETTER REVERSED OPEN E]
						case '\u025D': 
						// É�  [LATIN SMALL LETTER REVERSED OPEN E WITH HOOK]
						case '\u025E': 
						// Éž  [LATIN SMALL LETTER CLOSED REVERSED OPEN E]
						case '\u029A': 
						// Êš  [LATIN SMALL LETTER CLOSED OPEN E]
						case '\u1D08': 
						// á´ˆ  [LATIN SMALL LETTER TURNED OPEN E]
						case '\u1D92': 
						// á¶’  [LATIN SMALL LETTER E WITH RETROFLEX HOOK]
						case '\u1D93': 
						// á¶“  [LATIN SMALL LETTER OPEN E WITH RETROFLEX HOOK]
						case '\u1D94': 
						// á¶�?  [LATIN SMALL LETTER REVERSED OPEN E WITH RETROFLEX HOOK]
						case '\u1E15': 
						// á¸•  [LATIN SMALL LETTER E WITH MACRON AND GRAVE]
						case '\u1E17': 
						// á¸—  [LATIN SMALL LETTER E WITH MACRON AND ACUTE]
						case '\u1E19': 
						// á¸™  [LATIN SMALL LETTER E WITH CIRCUMFLEX BELOW]
						case '\u1E1B': 
						// á¸›  [LATIN SMALL LETTER E WITH TILDE BELOW]
						case '\u1E1D': 
						// á¸�  [LATIN SMALL LETTER E WITH CEDILLA AND BREVE]
						case '\u1EB9': 
						// áº¹  [LATIN SMALL LETTER E WITH DOT BELOW]
						case '\u1EBB': 
						// áº»  [LATIN SMALL LETTER E WITH HOOK ABOVE]
						case '\u1EBD': 
						// áº½  [LATIN SMALL LETTER E WITH TILDE]
						case '\u1EBF': 
						// áº¿  [LATIN SMALL LETTER E WITH CIRCUMFLEX AND ACUTE]
						case '\u1EC1': 
						// á»�  [LATIN SMALL LETTER E WITH CIRCUMFLEX AND GRAVE]
						case '\u1EC3': 
						// á»ƒ  [LATIN SMALL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE]
						case '\u1EC5': 
						// á»…  [LATIN SMALL LETTER E WITH CIRCUMFLEX AND TILDE]
						case '\u1EC7': 
						// á»‡  [LATIN SMALL LETTER E WITH CIRCUMFLEX AND DOT BELOW]
						case '\u2091': 
						// â‚‘  [LATIN SUBSCRIPT SMALL LETTER E]
						case '\u24D4': 
						// â“�?  [CIRCLED LATIN SMALL LETTER E]
						case '\u2C78': 
						// â±¸  [LATIN SMALL LETTER E WITH NOTCH]
						case '\uFF45':  // ï½…  [FULLWIDTH LATIN SMALL LETTER E]
							output[outputPos++] = 'e';
							break;
						
						case '\u24A0':  // â’   [PARENTHESIZED LATIN SMALL LETTER E]
							output[outputPos++] = '(';
							output[outputPos++] = 'e';
							output[outputPos++] = ')';
							break;
						
						case '\u0191': 
						// Æ‘  [LATIN CAPITAL LETTER F WITH HOOK]
						case '\u1E1E': 
						// á¸ž  [LATIN CAPITAL LETTER F WITH DOT ABOVE]
						case '\u24BB': 
						// â’»  [CIRCLED LATIN CAPITAL LETTER F]
						case '\uA730': 
						// êœ°  [LATIN LETTER SMALL CAPITAL F]
						case '\uA77B': 
						// ê�»  [LATIN CAPITAL LETTER INSULAR F]
						case '\uA7FB': 
						// êŸ»  [LATIN EPIGRAPHIC LETTER REVERSED F]
						case '\uFF26':  // ï¼¦  [FULLWIDTH LATIN CAPITAL LETTER F]
							output[outputPos++] = 'F';
							break;
						
						case '\u0192': 
						// Æ’  [LATIN SMALL LETTER F WITH HOOK]
						case '\u1D6E': 
						// áµ®  [LATIN SMALL LETTER F WITH MIDDLE TILDE]
						case '\u1D82': 
						// á¶‚  [LATIN SMALL LETTER F WITH PALATAL HOOK]
						case '\u1E1F': 
						// á¸Ÿ  [LATIN SMALL LETTER F WITH DOT ABOVE]
						case '\u1E9B': 
						// áº›  [LATIN SMALL LETTER LONG S WITH DOT ABOVE]
						case '\u24D5': 
						// â“•  [CIRCLED LATIN SMALL LETTER F]
						case '\uA77C': 
						// ê�¼  [LATIN SMALL LETTER INSULAR F]
						case '\uFF46':  // ï½†  [FULLWIDTH LATIN SMALL LETTER F]
							output[outputPos++] = 'f';
							break;
						
						case '\u24A1':  // â’¡  [PARENTHESIZED LATIN SMALL LETTER F]
							output[outputPos++] = '(';
							output[outputPos++] = 'f';
							output[outputPos++] = ')';
							break;
						
						case '\uFB00':  // ï¬€  [LATIN SMALL LIGATURE FF]
							output[outputPos++] = 'f';
							output[outputPos++] = 'f';
							break;
						
						case '\uFB03':  // ï¬ƒ  [LATIN SMALL LIGATURE FFI]
							output[outputPos++] = 'f';
							output[outputPos++] = 'f';
							output[outputPos++] = 'i';
							break;
						
						case '\uFB04':  // ï¬„  [LATIN SMALL LIGATURE FFL]
							output[outputPos++] = 'f';
							output[outputPos++] = 'f';
							output[outputPos++] = 'l';
							break;
						
						case '\uFB01':  // ï¬�  [LATIN SMALL LIGATURE FI]
							output[outputPos++] = 'f';
							output[outputPos++] = 'i';
							break;
						
						case '\uFB02':  // ï¬‚  [LATIN SMALL LIGATURE FL]
							output[outputPos++] = 'f';
							output[outputPos++] = 'l';
							break;
						
						case '\u011C': 
						// Äœ  [LATIN CAPITAL LETTER G WITH CIRCUMFLEX]
						case '\u011E': 
						// Äž  [LATIN CAPITAL LETTER G WITH BREVE]
						case '\u0120': 
						// Ä   [LATIN CAPITAL LETTER G WITH DOT ABOVE]
						case '\u0122': 
						// Ä¢  [LATIN CAPITAL LETTER G WITH CEDILLA]
						case '\u0193': 
						// Æ“  [LATIN CAPITAL LETTER G WITH HOOK]
						case '\u01E4': 
						// Ç¤  [LATIN CAPITAL LETTER G WITH STROKE]
						case '\u01E5': 
						// Ç¥  [LATIN SMALL LETTER G WITH STROKE]
						case '\u01E6': 
						// Ç¦  [LATIN CAPITAL LETTER G WITH CARON]
						case '\u01E7': 
						// Ç§  [LATIN SMALL LETTER G WITH CARON]
						case '\u01F4': 
						// Ç´  [LATIN CAPITAL LETTER G WITH ACUTE]
						case '\u0262': 
						// É¢  [LATIN LETTER SMALL CAPITAL G]
						case '\u029B': 
						// Ê›  [LATIN LETTER SMALL CAPITAL G WITH HOOK]
						case '\u1E20': 
						// á¸   [LATIN CAPITAL LETTER G WITH MACRON]
						case '\u24BC': 
						// â’¼  [CIRCLED LATIN CAPITAL LETTER G]
						case '\uA77D': 
						// ê�½  [LATIN CAPITAL LETTER INSULAR G]
						case '\uA77E': 
						// ê�¾  [LATIN CAPITAL LETTER TURNED INSULAR G]
						case '\uFF27':  // ï¼§  [FULLWIDTH LATIN CAPITAL LETTER G]
							output[outputPos++] = 'G';
							break;
						
						case '\u011D': 
						// Ä�  [LATIN SMALL LETTER G WITH CIRCUMFLEX]
						case '\u011F': 
						// ÄŸ  [LATIN SMALL LETTER G WITH BREVE]
						case '\u0121': 
						// Ä¡  [LATIN SMALL LETTER G WITH DOT ABOVE]
						case '\u0123': 
						// Ä£  [LATIN SMALL LETTER G WITH CEDILLA]
						case '\u01F5': 
						// Çµ  [LATIN SMALL LETTER G WITH ACUTE]
						case '\u0260': 
						// É   [LATIN SMALL LETTER G WITH HOOK]
						case '\u0261': 
						// É¡  [LATIN SMALL LETTER SCRIPT G]
						case '\u1D77': 
						// áµ·  [LATIN SMALL LETTER TURNED G]
						case '\u1D79': 
						// áµ¹  [LATIN SMALL LETTER INSULAR G]
						case '\u1D83': 
						// á¶ƒ  [LATIN SMALL LETTER G WITH PALATAL HOOK]
						case '\u1E21': 
						// á¸¡  [LATIN SMALL LETTER G WITH MACRON]
						case '\u24D6': 
						// â“–  [CIRCLED LATIN SMALL LETTER G]
						case '\uA77F': 
						// ê�¿  [LATIN SMALL LETTER TURNED INSULAR G]
						case '\uFF47':  // ï½‡  [FULLWIDTH LATIN SMALL LETTER G]
							output[outputPos++] = 'g';
							break;
						
						case '\u24A2':  // â’¢  [PARENTHESIZED LATIN SMALL LETTER G]
							output[outputPos++] = '(';
							output[outputPos++] = 'g';
							output[outputPos++] = ')';
							break;
						
						case '\u0124': 
						// Ä¤  [LATIN CAPITAL LETTER H WITH CIRCUMFLEX]
						case '\u0126': 
						// Ä¦  [LATIN CAPITAL LETTER H WITH STROKE]
						case '\u021E': 
						// Èž  [LATIN CAPITAL LETTER H WITH CARON]
						case '\u029C': 
						// Êœ  [LATIN LETTER SMALL CAPITAL H]
						case '\u1E22': 
						// á¸¢  [LATIN CAPITAL LETTER H WITH DOT ABOVE]
						case '\u1E24': 
						// á¸¤  [LATIN CAPITAL LETTER H WITH DOT BELOW]
						case '\u1E26': 
						// á¸¦  [LATIN CAPITAL LETTER H WITH DIAERESIS]
						case '\u1E28': 
						// á¸¨  [LATIN CAPITAL LETTER H WITH CEDILLA]
						case '\u1E2A': 
						// á¸ª  [LATIN CAPITAL LETTER H WITH BREVE BELOW]
						case '\u24BD': 
						// â’½  [CIRCLED LATIN CAPITAL LETTER H]
						case '\u2C67': 
						// â±§  [LATIN CAPITAL LETTER H WITH DESCENDER]
						case '\u2C75': 
						// â±µ  [LATIN CAPITAL LETTER HALF H]
						case '\uFF28':  // ï¼¨  [FULLWIDTH LATIN CAPITAL LETTER H]
							output[outputPos++] = 'H';
							break;
						
						case '\u0125': 
						// Ä¥  [LATIN SMALL LETTER H WITH CIRCUMFLEX]
						case '\u0127': 
						// Ä§  [LATIN SMALL LETTER H WITH STROKE]
						case '\u021F': 
						// ÈŸ  [LATIN SMALL LETTER H WITH CARON]
						case '\u0265': 
						// É¥  [LATIN SMALL LETTER TURNED H]
						case '\u0266': 
						// É¦  [LATIN SMALL LETTER H WITH HOOK]
						case '\u02AE': 
						// Ê®  [LATIN SMALL LETTER TURNED H WITH FISHHOOK]
						case '\u02AF': 
						// Ê¯  [LATIN SMALL LETTER TURNED H WITH FISHHOOK AND TAIL]
						case '\u1E23': 
						// á¸£  [LATIN SMALL LETTER H WITH DOT ABOVE]
						case '\u1E25': 
						// á¸¥  [LATIN SMALL LETTER H WITH DOT BELOW]
						case '\u1E27': 
						// á¸§  [LATIN SMALL LETTER H WITH DIAERESIS]
						case '\u1E29': 
						// á¸©  [LATIN SMALL LETTER H WITH CEDILLA]
						case '\u1E2B': 
						// á¸«  [LATIN SMALL LETTER H WITH BREVE BELOW]
						case '\u1E96': 
						// áº–  [LATIN SMALL LETTER H WITH LINE BELOW]
						case '\u24D7': 
						// â“—  [CIRCLED LATIN SMALL LETTER H]
						case '\u2C68': 
						// â±¨  [LATIN SMALL LETTER H WITH DESCENDER]
						case '\u2C76': 
						// â±¶  [LATIN SMALL LETTER HALF H]
						case '\uFF48':  // ï½ˆ  [FULLWIDTH LATIN SMALL LETTER H]
							output[outputPos++] = 'h';
							break;
						
						case '\u01F6':  // Ç¶  http://en.wikipedia.org/wiki/Hwair  [LATIN CAPITAL LETTER HWAIR]
							output[outputPos++] = 'H';
							output[outputPos++] = 'V';
							break;
						
						case '\u24A3':  // â’£  [PARENTHESIZED LATIN SMALL LETTER H]
							output[outputPos++] = '(';
							output[outputPos++] = 'h';
							output[outputPos++] = ')';
							break;
						
						case '\u0195':  // Æ•  [LATIN SMALL LETTER HV]
							output[outputPos++] = 'h';
							output[outputPos++] = 'v';
							break;
						
						case '\u00CC': 
						// ÃŒ  [LATIN CAPITAL LETTER I WITH GRAVE]
						case '\u00CD': 
						// Ã�  [LATIN CAPITAL LETTER I WITH ACUTE]
						case '\u00CE': 
						// ÃŽ  [LATIN CAPITAL LETTER I WITH CIRCUMFLEX]
						case '\u00CF': 
						// Ã�  [LATIN CAPITAL LETTER I WITH DIAERESIS]
						case '\u0128': 
						// Ä¨  [LATIN CAPITAL LETTER I WITH TILDE]
						case '\u012A': 
						// Äª  [LATIN CAPITAL LETTER I WITH MACRON]
						case '\u012C': 
						// Ä¬  [LATIN CAPITAL LETTER I WITH BREVE]
						case '\u012E': 
						// Ä®  [LATIN CAPITAL LETTER I WITH OGONEK]
						case '\u0130': 
						// Ä°  [LATIN CAPITAL LETTER I WITH DOT ABOVE]
						case '\u0196': 
						// Æ–  [LATIN CAPITAL LETTER IOTA]
						case '\u0197': 
						// Æ—  [LATIN CAPITAL LETTER I WITH STROKE]
						case '\u01CF': 
						// Ç�  [LATIN CAPITAL LETTER I WITH CARON]
						case '\u0208': 
						// Èˆ  [LATIN CAPITAL LETTER I WITH DOUBLE GRAVE]
						case '\u020A': 
						// ÈŠ  [LATIN CAPITAL LETTER I WITH INVERTED BREVE]
						case '\u026A': 
						// Éª  [LATIN LETTER SMALL CAPITAL I]
						case '\u1D7B': 
						// áµ»  [LATIN SMALL CAPITAL LETTER I WITH STROKE]
						case '\u1E2C': 
						// á¸¬  [LATIN CAPITAL LETTER I WITH TILDE BELOW]
						case '\u1E2E': 
						// á¸®  [LATIN CAPITAL LETTER I WITH DIAERESIS AND ACUTE]
						case '\u1EC8': 
						// á»ˆ  [LATIN CAPITAL LETTER I WITH HOOK ABOVE]
						case '\u1ECA': 
						// á»Š  [LATIN CAPITAL LETTER I WITH DOT BELOW]
						case '\u24BE': 
						// â’¾  [CIRCLED LATIN CAPITAL LETTER I]
						case '\uA7FE': 
						// êŸ¾  [LATIN EPIGRAPHIC LETTER I LONGA]
						case '\uFF29':  // ï¼©  [FULLWIDTH LATIN CAPITAL LETTER I]
							output[outputPos++] = 'I';
							break;
						
						case '\u00EC': 
						// Ã¬  [LATIN SMALL LETTER I WITH GRAVE]
						case '\u00ED': 
						// Ã­  [LATIN SMALL LETTER I WITH ACUTE]
						case '\u00EE': 
						// Ã®  [LATIN SMALL LETTER I WITH CIRCUMFLEX]
						case '\u00EF': 
						// Ã¯  [LATIN SMALL LETTER I WITH DIAERESIS]
						case '\u0129': 
						// Ä©  [LATIN SMALL LETTER I WITH TILDE]
						case '\u012B': 
						// Ä«  [LATIN SMALL LETTER I WITH MACRON]
						case '\u012D': 
						// Ä­  [LATIN SMALL LETTER I WITH BREVE]
						case '\u012F': 
						// Ä¯  [LATIN SMALL LETTER I WITH OGONEK]
						case '\u0131': 
						// Ä±  [LATIN SMALL LETTER DOTLESS I]
						case '\u01D0': 
						// Ç�  [LATIN SMALL LETTER I WITH CARON]
						case '\u0209': 
						// È‰  [LATIN SMALL LETTER I WITH DOUBLE GRAVE]
						case '\u020B': 
						// È‹  [LATIN SMALL LETTER I WITH INVERTED BREVE]
						case '\u0268': 
						// É¨  [LATIN SMALL LETTER I WITH STROKE]
						case '\u1D09': 
						// á´‰  [LATIN SMALL LETTER TURNED I]
						case '\u1D62': 
						// áµ¢  [LATIN SUBSCRIPT SMALL LETTER I]
						case '\u1D7C': 
						// áµ¼  [LATIN SMALL LETTER IOTA WITH STROKE]
						case '\u1D96': 
						// á¶–  [LATIN SMALL LETTER I WITH RETROFLEX HOOK]
						case '\u1E2D': 
						// á¸­  [LATIN SMALL LETTER I WITH TILDE BELOW]
						case '\u1E2F': 
						// á¸¯  [LATIN SMALL LETTER I WITH DIAERESIS AND ACUTE]
						case '\u1EC9': 
						// á»‰  [LATIN SMALL LETTER I WITH HOOK ABOVE]
						case '\u1ECB': 
						// á»‹  [LATIN SMALL LETTER I WITH DOT BELOW]
						case '\u2071': 
						// â�±  [SUPERSCRIPT LATIN SMALL LETTER I]
						case '\u24D8': 
						// â“˜  [CIRCLED LATIN SMALL LETTER I]
						case '\uFF49':  // ï½‰  [FULLWIDTH LATIN SMALL LETTER I]
							output[outputPos++] = 'i';
							break;
						
						case '\u0132':  // Ä²  [LATIN CAPITAL LIGATURE IJ]
							output[outputPos++] = 'I';
							output[outputPos++] = 'J';
							break;
						
						case '\u24A4':  // â’¤  [PARENTHESIZED LATIN SMALL LETTER I]
							output[outputPos++] = '(';
							output[outputPos++] = 'i';
							output[outputPos++] = ')';
							break;
						
						case '\u0133':  // Ä³  [LATIN SMALL LIGATURE IJ]
							output[outputPos++] = 'i';
							output[outputPos++] = 'j';
							break;
						
						case '\u0134': 
						// Ä´  [LATIN CAPITAL LETTER J WITH CIRCUMFLEX]
						case '\u0248': 
						// Éˆ  [LATIN CAPITAL LETTER J WITH STROKE]
						case '\u1D0A': 
						// á´Š  [LATIN LETTER SMALL CAPITAL J]
						case '\u24BF': 
						// â’¿  [CIRCLED LATIN CAPITAL LETTER J]
						case '\uFF2A':  // ï¼ª  [FULLWIDTH LATIN CAPITAL LETTER J]
							output[outputPos++] = 'J';
							break;
						
						case '\u0135': 
						// Äµ  [LATIN SMALL LETTER J WITH CIRCUMFLEX]
						case '\u01F0': 
						// Ç°  [LATIN SMALL LETTER J WITH CARON]
						case '\u0237': 
						// È·  [LATIN SMALL LETTER DOTLESS J]
						case '\u0249': 
						// É‰  [LATIN SMALL LETTER J WITH STROKE]
						case '\u025F': 
						// ÉŸ  [LATIN SMALL LETTER DOTLESS J WITH STROKE]
						case '\u0284': 
						// Ê„  [LATIN SMALL LETTER DOTLESS J WITH STROKE AND HOOK]
						case '\u029D': 
						// Ê�  [LATIN SMALL LETTER J WITH CROSSED-TAIL]
						case '\u24D9': 
						// â“™  [CIRCLED LATIN SMALL LETTER J]
						case '\u2C7C': 
						// â±¼  [LATIN SUBSCRIPT SMALL LETTER J]
						case '\uFF4A':  // ï½Š  [FULLWIDTH LATIN SMALL LETTER J]
							output[outputPos++] = 'j';
							break;
						
						case '\u24A5':  // â’¥  [PARENTHESIZED LATIN SMALL LETTER J]
							output[outputPos++] = '(';
							output[outputPos++] = 'j';
							output[outputPos++] = ')';
							break;
						
						case '\u0136': 
						// Ä¶  [LATIN CAPITAL LETTER K WITH CEDILLA]
						case '\u0198': 
						// Æ˜  [LATIN CAPITAL LETTER K WITH HOOK]
						case '\u01E8': 
						// Ç¨  [LATIN CAPITAL LETTER K WITH CARON]
						case '\u1D0B': 
						// á´‹  [LATIN LETTER SMALL CAPITAL K]
						case '\u1E30': 
						// á¸°  [LATIN CAPITAL LETTER K WITH ACUTE]
						case '\u1E32': 
						// á¸²  [LATIN CAPITAL LETTER K WITH DOT BELOW]
						case '\u1E34': 
						// á¸´  [LATIN CAPITAL LETTER K WITH LINE BELOW]
						case '\u24C0': 
						// â“€  [CIRCLED LATIN CAPITAL LETTER K]
						case '\u2C69': 
						// â±©  [LATIN CAPITAL LETTER K WITH DESCENDER]
						case '\uA740': 
						// ê�€  [LATIN CAPITAL LETTER K WITH STROKE]
						case '\uA742': 
						// ê�‚  [LATIN CAPITAL LETTER K WITH DIAGONAL STROKE]
						case '\uA744': 
						// ê�„  [LATIN CAPITAL LETTER K WITH STROKE AND DIAGONAL STROKE]
						case '\uFF2B':  // ï¼«  [FULLWIDTH LATIN CAPITAL LETTER K]
							output[outputPos++] = 'K';
							break;
						
						case '\u0137': 
						// Ä·  [LATIN SMALL LETTER K WITH CEDILLA]
						case '\u0199': 
						// Æ™  [LATIN SMALL LETTER K WITH HOOK]
						case '\u01E9': 
						// Ç©  [LATIN SMALL LETTER K WITH CARON]
						case '\u029E': 
						// Êž  [LATIN SMALL LETTER TURNED K]
						case '\u1D84': 
						// á¶„  [LATIN SMALL LETTER K WITH PALATAL HOOK]
						case '\u1E31': 
						// á¸±  [LATIN SMALL LETTER K WITH ACUTE]
						case '\u1E33': 
						// á¸³  [LATIN SMALL LETTER K WITH DOT BELOW]
						case '\u1E35': 
						// á¸µ  [LATIN SMALL LETTER K WITH LINE BELOW]
						case '\u24DA': 
						// â“š  [CIRCLED LATIN SMALL LETTER K]
						case '\u2C6A': 
						// â±ª  [LATIN SMALL LETTER K WITH DESCENDER]
						case '\uA741': 
						// ê��  [LATIN SMALL LETTER K WITH STROKE]
						case '\uA743': 
						// ê�ƒ  [LATIN SMALL LETTER K WITH DIAGONAL STROKE]
						case '\uA745': 
						// ê�…  [LATIN SMALL LETTER K WITH STROKE AND DIAGONAL STROKE]
						case '\uFF4B':  // ï½‹  [FULLWIDTH LATIN SMALL LETTER K]
							output[outputPos++] = 'k';
							break;
						
						case '\u24A6':  // â’¦  [PARENTHESIZED LATIN SMALL LETTER K]
							output[outputPos++] = '(';
							output[outputPos++] = 'k';
							output[outputPos++] = ')';
							break;
						
						case '\u0139': 
						// Ä¹  [LATIN CAPITAL LETTER L WITH ACUTE]
						case '\u013B': 
						// Ä»  [LATIN CAPITAL LETTER L WITH CEDILLA]
						case '\u013D': 
						// Ä½  [LATIN CAPITAL LETTER L WITH CARON]
						case '\u013F': 
						// Ä¿  [LATIN CAPITAL LETTER L WITH MIDDLE DOT]
						case '\u0141': 
						// Å�  [LATIN CAPITAL LETTER L WITH STROKE]
						case '\u023D': 
						// È½  [LATIN CAPITAL LETTER L WITH BAR]
						case '\u029F': 
						// ÊŸ  [LATIN LETTER SMALL CAPITAL L]
						case '\u1D0C': 
						// á´Œ  [LATIN LETTER SMALL CAPITAL L WITH STROKE]
						case '\u1E36': 
						// á¸¶  [LATIN CAPITAL LETTER L WITH DOT BELOW]
						case '\u1E38': 
						// á¸¸  [LATIN CAPITAL LETTER L WITH DOT BELOW AND MACRON]
						case '\u1E3A': 
						// á¸º  [LATIN CAPITAL LETTER L WITH LINE BELOW]
						case '\u1E3C': 
						// á¸¼  [LATIN CAPITAL LETTER L WITH CIRCUMFLEX BELOW]
						case '\u24C1': 
						// â“�  [CIRCLED LATIN CAPITAL LETTER L]
						case '\u2C60': 
						// â±   [LATIN CAPITAL LETTER L WITH DOUBLE BAR]
						case '\u2C62': 
						// â±¢  [LATIN CAPITAL LETTER L WITH MIDDLE TILDE]
						case '\uA746': 
						// ê�†  [LATIN CAPITAL LETTER BROKEN L]
						case '\uA748': 
						// ê�ˆ  [LATIN CAPITAL LETTER L WITH HIGH STROKE]
						case '\uA780': 
						// êž€  [LATIN CAPITAL LETTER TURNED L]
						case '\uFF2C':  // ï¼¬  [FULLWIDTH LATIN CAPITAL LETTER L]
							output[outputPos++] = 'L';
							break;
						
						case '\u013A': 
						// Äº  [LATIN SMALL LETTER L WITH ACUTE]
						case '\u013C': 
						// Ä¼  [LATIN SMALL LETTER L WITH CEDILLA]
						case '\u013E': 
						// Ä¾  [LATIN SMALL LETTER L WITH CARON]
						case '\u0140': 
						// Å€  [LATIN SMALL LETTER L WITH MIDDLE DOT]
						case '\u0142': 
						// Å‚  [LATIN SMALL LETTER L WITH STROKE]
						case '\u019A': 
						// Æš  [LATIN SMALL LETTER L WITH BAR]
						case '\u0234': 
						// È´  [LATIN SMALL LETTER L WITH CURL]
						case '\u026B': 
						// É«  [LATIN SMALL LETTER L WITH MIDDLE TILDE]
						case '\u026C': 
						// É¬  [LATIN SMALL LETTER L WITH BELT]
						case '\u026D': 
						// É­  [LATIN SMALL LETTER L WITH RETROFLEX HOOK]
						case '\u1D85': 
						// á¶…  [LATIN SMALL LETTER L WITH PALATAL HOOK]
						case '\u1E37': 
						// á¸·  [LATIN SMALL LETTER L WITH DOT BELOW]
						case '\u1E39': 
						// á¸¹  [LATIN SMALL LETTER L WITH DOT BELOW AND MACRON]
						case '\u1E3B': 
						// á¸»  [LATIN SMALL LETTER L WITH LINE BELOW]
						case '\u1E3D': 
						// á¸½  [LATIN SMALL LETTER L WITH CIRCUMFLEX BELOW]
						case '\u24DB': 
						// â“›  [CIRCLED LATIN SMALL LETTER L]
						case '\u2C61': 
						// â±¡  [LATIN SMALL LETTER L WITH DOUBLE BAR]
						case '\uA747': 
						// ê�‡  [LATIN SMALL LETTER BROKEN L]
						case '\uA749': 
						// ê�‰  [LATIN SMALL LETTER L WITH HIGH STROKE]
						case '\uA781': 
						// êž�  [LATIN SMALL LETTER TURNED L]
						case '\uFF4C':  // ï½Œ  [FULLWIDTH LATIN SMALL LETTER L]
							output[outputPos++] = 'l';
							break;
						
						case '\u01C7':  // Ç‡  [LATIN CAPITAL LETTER LJ]
							output[outputPos++] = 'L';
							output[outputPos++] = 'J';
							break;
						
						case '\u1EFA':  // á»º  [LATIN CAPITAL LETTER MIDDLE-WELSH LL]
							output[outputPos++] = 'L';
							output[outputPos++] = 'L';
							break;
						
						case '\u01C8':  // Çˆ  [LATIN CAPITAL LETTER L WITH SMALL LETTER J]
							output[outputPos++] = 'L';
							output[outputPos++] = 'j';
							break;
						
						case '\u24A7':  // â’§  [PARENTHESIZED LATIN SMALL LETTER L]
							output[outputPos++] = '(';
							output[outputPos++] = 'l';
							output[outputPos++] = ')';
							break;
						
						case '\u01C9':  // Ç‰  [LATIN SMALL LETTER LJ]
							output[outputPos++] = 'l';
							output[outputPos++] = 'j';
							break;
						
						case '\u1EFB':  // á»»  [LATIN SMALL LETTER MIDDLE-WELSH LL]
							output[outputPos++] = 'l';
							output[outputPos++] = 'l';
							break;
						
						case '\u02AA':  // Êª  [LATIN SMALL LETTER LS DIGRAPH]
							output[outputPos++] = 'l';
							output[outputPos++] = 's';
							break;
						
						case '\u02AB':  // Ê«  [LATIN SMALL LETTER LZ DIGRAPH]
							output[outputPos++] = 'l';
							output[outputPos++] = 'z';
							break;
						
						case '\u019C': 
						// Æœ  [LATIN CAPITAL LETTER TURNED M]
						case '\u1D0D': 
						// á´�  [LATIN LETTER SMALL CAPITAL M]
						case '\u1E3E': 
						// á¸¾  [LATIN CAPITAL LETTER M WITH ACUTE]
						case '\u1E40': 
						// á¹€  [LATIN CAPITAL LETTER M WITH DOT ABOVE]
						case '\u1E42': 
						// á¹‚  [LATIN CAPITAL LETTER M WITH DOT BELOW]
						case '\u24C2': 
						// â“‚  [CIRCLED LATIN CAPITAL LETTER M]
						case '\u2C6E': 
						// â±®  [LATIN CAPITAL LETTER M WITH HOOK]
						case '\uA7FD': 
						// êŸ½  [LATIN EPIGRAPHIC LETTER INVERTED M]
						case '\uA7FF': 
						// êŸ¿  [LATIN EPIGRAPHIC LETTER ARCHAIC M]
						case '\uFF2D':  // ï¼­  [FULLWIDTH LATIN CAPITAL LETTER M]
							output[outputPos++] = 'M';
							break;
						
						case '\u026F': 
						// É¯  [LATIN SMALL LETTER TURNED M]
						case '\u0270': 
						// É°  [LATIN SMALL LETTER TURNED M WITH LONG LEG]
						case '\u0271': 
						// É±  [LATIN SMALL LETTER M WITH HOOK]
						case '\u1D6F': 
						// áµ¯  [LATIN SMALL LETTER M WITH MIDDLE TILDE]
						case '\u1D86': 
						// á¶†  [LATIN SMALL LETTER M WITH PALATAL HOOK]
						case '\u1E3F': 
						// á¸¿  [LATIN SMALL LETTER M WITH ACUTE]
						case '\u1E41': 
						// á¹�  [LATIN SMALL LETTER M WITH DOT ABOVE]
						case '\u1E43': 
						// á¹ƒ  [LATIN SMALL LETTER M WITH DOT BELOW]
						case '\u24DC': 
						// â“œ  [CIRCLED LATIN SMALL LETTER M]
						case '\uFF4D':  // ï½�  [FULLWIDTH LATIN SMALL LETTER M]
							output[outputPos++] = 'm';
							break;
						
						case '\u24A8':  // â’¨  [PARENTHESIZED LATIN SMALL LETTER M]
							output[outputPos++] = '(';
							output[outputPos++] = 'm';
							output[outputPos++] = ')';
							break;
						
						case '\u00D1': 
						// Ã‘  [LATIN CAPITAL LETTER N WITH TILDE]
						case '\u0143': 
						// Åƒ  [LATIN CAPITAL LETTER N WITH ACUTE]
						case '\u0145': 
						// Å…  [LATIN CAPITAL LETTER N WITH CEDILLA]
						case '\u0147': 
						// Å‡  [LATIN CAPITAL LETTER N WITH CARON]
						case '\u014A': 
						// ÅŠ  http://en.wikipedia.org/wiki/Eng_(letter)  [LATIN CAPITAL LETTER ENG]
						case '\u019D': 
						// Æ�  [LATIN CAPITAL LETTER N WITH LEFT HOOK]
						case '\u01F8': 
						// Ç¸  [LATIN CAPITAL LETTER N WITH GRAVE]
						case '\u0220': 
						// È   [LATIN CAPITAL LETTER N WITH LONG RIGHT LEG]
						case '\u0274': 
						// É´  [LATIN LETTER SMALL CAPITAL N]
						case '\u1D0E': 
						// á´Ž  [LATIN LETTER SMALL CAPITAL REVERSED N]
						case '\u1E44': 
						// á¹„  [LATIN CAPITAL LETTER N WITH DOT ABOVE]
						case '\u1E46': 
						// á¹†  [LATIN CAPITAL LETTER N WITH DOT BELOW]
						case '\u1E48': 
						// á¹ˆ  [LATIN CAPITAL LETTER N WITH LINE BELOW]
						case '\u1E4A': 
						// á¹Š  [LATIN CAPITAL LETTER N WITH CIRCUMFLEX BELOW]
						case '\u24C3': 
						// â“ƒ  [CIRCLED LATIN CAPITAL LETTER N]
						case '\uFF2E':  // ï¼®  [FULLWIDTH LATIN CAPITAL LETTER N]
							output[outputPos++] = 'N';
							break;
						
						case '\u00F1': 
						// Ã±  [LATIN SMALL LETTER N WITH TILDE]
						case '\u0144': 
						// Å„  [LATIN SMALL LETTER N WITH ACUTE]
						case '\u0146': 
						// Å†  [LATIN SMALL LETTER N WITH CEDILLA]
						case '\u0148': 
						// Åˆ  [LATIN SMALL LETTER N WITH CARON]
						case '\u0149': 
						// Å‰  [LATIN SMALL LETTER N PRECEDED BY APOSTROPHE]
						case '\u014B': 
						// Å‹  http://en.wikipedia.org/wiki/Eng_(letter)  [LATIN SMALL LETTER ENG]
						case '\u019E': 
						// Æž  [LATIN SMALL LETTER N WITH LONG RIGHT LEG]
						case '\u01F9': 
						// Ç¹  [LATIN SMALL LETTER N WITH GRAVE]
						case '\u0235': 
						// Èµ  [LATIN SMALL LETTER N WITH CURL]
						case '\u0272': 
						// É²  [LATIN SMALL LETTER N WITH LEFT HOOK]
						case '\u0273': 
						// É³  [LATIN SMALL LETTER N WITH RETROFLEX HOOK]
						case '\u1D70': 
						// áµ°  [LATIN SMALL LETTER N WITH MIDDLE TILDE]
						case '\u1D87': 
						// á¶‡  [LATIN SMALL LETTER N WITH PALATAL HOOK]
						case '\u1E45': 
						// á¹…  [LATIN SMALL LETTER N WITH DOT ABOVE]
						case '\u1E47': 
						// á¹‡  [LATIN SMALL LETTER N WITH DOT BELOW]
						case '\u1E49': 
						// á¹‰  [LATIN SMALL LETTER N WITH LINE BELOW]
						case '\u1E4B': 
						// á¹‹  [LATIN SMALL LETTER N WITH CIRCUMFLEX BELOW]
						case '\u207F': 
						// â�¿  [SUPERSCRIPT LATIN SMALL LETTER N]
						case '\u24DD': 
						// â“�  [CIRCLED LATIN SMALL LETTER N]
						case '\uFF4E':  // ï½Ž  [FULLWIDTH LATIN SMALL LETTER N]
							output[outputPos++] = 'n';
							break;
						
						case '\u01CA':  // ÇŠ  [LATIN CAPITAL LETTER NJ]
							output[outputPos++] = 'N';
							output[outputPos++] = 'J';
							break;
						
						case '\u01CB':  // Ç‹  [LATIN CAPITAL LETTER N WITH SMALL LETTER J]
							output[outputPos++] = 'N';
							output[outputPos++] = 'j';
							break;
						
						case '\u24A9':  // â’©  [PARENTHESIZED LATIN SMALL LETTER N]
							output[outputPos++] = '(';
							output[outputPos++] = 'n';
							output[outputPos++] = ')';
							break;
						
						case '\u01CC':  // ÇŒ  [LATIN SMALL LETTER NJ]
							output[outputPos++] = 'n';
							output[outputPos++] = 'j';
							break;
						
						case '\u00D2': 
						// Ã’  [LATIN CAPITAL LETTER O WITH GRAVE]
						case '\u00D3': 
						// Ã“  [LATIN CAPITAL LETTER O WITH ACUTE]
						case '\u00D4': 
						// Ã�?  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX]
						case '\u00D5': 
						// Ã•  [LATIN CAPITAL LETTER O WITH TILDE]
						case '\u00D6': 
						// Ã–  [LATIN CAPITAL LETTER O WITH DIAERESIS]
						case '\u00D8': 
						// Ã˜  [LATIN CAPITAL LETTER O WITH STROKE]
						case '\u014C': 
						// ÅŒ  [LATIN CAPITAL LETTER O WITH MACRON]
						case '\u014E': 
						// ÅŽ  [LATIN CAPITAL LETTER O WITH BREVE]
						case '\u0150': 
						// Å�  [LATIN CAPITAL LETTER O WITH DOUBLE ACUTE]
						case '\u0186': 
						// Æ†  [LATIN CAPITAL LETTER OPEN O]
						case '\u019F': 
						// ÆŸ  [LATIN CAPITAL LETTER O WITH MIDDLE TILDE]
						case '\u01A0': 
						// Æ   [LATIN CAPITAL LETTER O WITH HORN]
						case '\u01D1': 
						// Ç‘  [LATIN CAPITAL LETTER O WITH CARON]
						case '\u01EA': 
						// Çª  [LATIN CAPITAL LETTER O WITH OGONEK]
						case '\u01EC': 
						// Ç¬  [LATIN CAPITAL LETTER O WITH OGONEK AND MACRON]
						case '\u01FE': 
						// Ç¾  [LATIN CAPITAL LETTER O WITH STROKE AND ACUTE]
						case '\u020C': 
						// ÈŒ  [LATIN CAPITAL LETTER O WITH DOUBLE GRAVE]
						case '\u020E': 
						// ÈŽ  [LATIN CAPITAL LETTER O WITH INVERTED BREVE]
						case '\u022A': 
						// Èª  [LATIN CAPITAL LETTER O WITH DIAERESIS AND MACRON]
						case '\u022C': 
						// È¬  [LATIN CAPITAL LETTER O WITH TILDE AND MACRON]
						case '\u022E': 
						// È®  [LATIN CAPITAL LETTER O WITH DOT ABOVE]
						case '\u0230': 
						// È°  [LATIN CAPITAL LETTER O WITH DOT ABOVE AND MACRON]
						case '\u1D0F': 
						// á´�  [LATIN LETTER SMALL CAPITAL O]
						case '\u1D10': 
						// á´�  [LATIN LETTER SMALL CAPITAL OPEN O]
						case '\u1E4C': 
						// á¹Œ  [LATIN CAPITAL LETTER O WITH TILDE AND ACUTE]
						case '\u1E4E': 
						// á¹Ž  [LATIN CAPITAL LETTER O WITH TILDE AND DIAERESIS]
						case '\u1E50': 
						// á¹�  [LATIN CAPITAL LETTER O WITH MACRON AND GRAVE]
						case '\u1E52': 
						// á¹’  [LATIN CAPITAL LETTER O WITH MACRON AND ACUTE]
						case '\u1ECC': 
						// á»Œ  [LATIN CAPITAL LETTER O WITH DOT BELOW]
						case '\u1ECE': 
						// á»Ž  [LATIN CAPITAL LETTER O WITH HOOK ABOVE]
						case '\u1ED0': 
						// á»�  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND ACUTE]
						case '\u1ED2': 
						// á»’  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND GRAVE]
						case '\u1ED4': 
						// á»�?  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE]
						case '\u1ED6': 
						// á»–  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND TILDE]
						case '\u1ED8': 
						// á»˜  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND DOT BELOW]
						case '\u1EDA': 
						// á»š  [LATIN CAPITAL LETTER O WITH HORN AND ACUTE]
						case '\u1EDC': 
						// á»œ  [LATIN CAPITAL LETTER O WITH HORN AND GRAVE]
						case '\u1EDE': 
						// á»ž  [LATIN CAPITAL LETTER O WITH HORN AND HOOK ABOVE]
						case '\u1EE0': 
						// á»   [LATIN CAPITAL LETTER O WITH HORN AND TILDE]
						case '\u1EE2': 
						// á»¢  [LATIN CAPITAL LETTER O WITH HORN AND DOT BELOW]
						case '\u24C4': 
						// â“„  [CIRCLED LATIN CAPITAL LETTER O]
						case '\uA74A': 
						// ê�Š  [LATIN CAPITAL LETTER O WITH LONG STROKE OVERLAY]
						case '\uA74C': 
						// ê�Œ  [LATIN CAPITAL LETTER O WITH LOOP]
						case '\uFF2F':  // ï¼¯  [FULLWIDTH LATIN CAPITAL LETTER O]
							output[outputPos++] = 'O';
							break;
						
						case '\u00F2': 
						// Ã²  [LATIN SMALL LETTER O WITH GRAVE]
						case '\u00F3': 
						// Ã³  [LATIN SMALL LETTER O WITH ACUTE]
						case '\u00F4': 
						// Ã´  [LATIN SMALL LETTER O WITH CIRCUMFLEX]
						case '\u00F5': 
						// Ãµ  [LATIN SMALL LETTER O WITH TILDE]
						case '\u00F6': 
						// Ã¶  [LATIN SMALL LETTER O WITH DIAERESIS]
						case '\u00F8': 
						// Ã¸  [LATIN SMALL LETTER O WITH STROKE]
						case '\u014D': 
						// Å�  [LATIN SMALL LETTER O WITH MACRON]
						case '\u014F': 
						// Å�  [LATIN SMALL LETTER O WITH BREVE]
						case '\u0151': 
						// Å‘  [LATIN SMALL LETTER O WITH DOUBLE ACUTE]
						case '\u01A1': 
						// Æ¡  [LATIN SMALL LETTER O WITH HORN]
						case '\u01D2': 
						// Ç’  [LATIN SMALL LETTER O WITH CARON]
						case '\u01EB': 
						// Ç«  [LATIN SMALL LETTER O WITH OGONEK]
						case '\u01ED': 
						// Ç­  [LATIN SMALL LETTER O WITH OGONEK AND MACRON]
						case '\u01FF': 
						// Ç¿  [LATIN SMALL LETTER O WITH STROKE AND ACUTE]
						case '\u020D': 
						// È�  [LATIN SMALL LETTER O WITH DOUBLE GRAVE]
						case '\u020F': 
						// È�  [LATIN SMALL LETTER O WITH INVERTED BREVE]
						case '\u022B': 
						// È«  [LATIN SMALL LETTER O WITH DIAERESIS AND MACRON]
						case '\u022D': 
						// È­  [LATIN SMALL LETTER O WITH TILDE AND MACRON]
						case '\u022F': 
						// È¯  [LATIN SMALL LETTER O WITH DOT ABOVE]
						case '\u0231': 
						// È±  [LATIN SMALL LETTER O WITH DOT ABOVE AND MACRON]
						case '\u0254': 
						// É�?  [LATIN SMALL LETTER OPEN O]
						case '\u0275': 
						// Éµ  [LATIN SMALL LETTER BARRED O]
						case '\u1D16': 
						// á´–  [LATIN SMALL LETTER TOP HALF O]
						case '\u1D17': 
						// á´—  [LATIN SMALL LETTER BOTTOM HALF O]
						case '\u1D97': 
						// á¶—  [LATIN SMALL LETTER OPEN O WITH RETROFLEX HOOK]
						case '\u1E4D': 
						// á¹�  [LATIN SMALL LETTER O WITH TILDE AND ACUTE]
						case '\u1E4F': 
						// á¹�  [LATIN SMALL LETTER O WITH TILDE AND DIAERESIS]
						case '\u1E51': 
						// á¹‘  [LATIN SMALL LETTER O WITH MACRON AND GRAVE]
						case '\u1E53': 
						// á¹“  [LATIN SMALL LETTER O WITH MACRON AND ACUTE]
						case '\u1ECD': 
						// á»�  [LATIN SMALL LETTER O WITH DOT BELOW]
						case '\u1ECF': 
						// á»�  [LATIN SMALL LETTER O WITH HOOK ABOVE]
						case '\u1ED1': 
						// á»‘  [LATIN SMALL LETTER O WITH CIRCUMFLEX AND ACUTE]
						case '\u1ED3': 
						// á»“  [LATIN SMALL LETTER O WITH CIRCUMFLEX AND GRAVE]
						case '\u1ED5': 
						// á»•  [LATIN SMALL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE]
						case '\u1ED7': 
						// á»—  [LATIN SMALL LETTER O WITH CIRCUMFLEX AND TILDE]
						case '\u1ED9': 
						// á»™  [LATIN SMALL LETTER O WITH CIRCUMFLEX AND DOT BELOW]
						case '\u1EDB': 
						// á»›  [LATIN SMALL LETTER O WITH HORN AND ACUTE]
						case '\u1EDD': 
						// á»�  [LATIN SMALL LETTER O WITH HORN AND GRAVE]
						case '\u1EDF': 
						// á»Ÿ  [LATIN SMALL LETTER O WITH HORN AND HOOK ABOVE]
						case '\u1EE1': 
						// á»¡  [LATIN SMALL LETTER O WITH HORN AND TILDE]
						case '\u1EE3': 
						// á»£  [LATIN SMALL LETTER O WITH HORN AND DOT BELOW]
						case '\u2092': 
						// â‚’  [LATIN SUBSCRIPT SMALL LETTER O]
						case '\u24DE': 
						// â“ž  [CIRCLED LATIN SMALL LETTER O]
						case '\u2C7A': 
						// â±º  [LATIN SMALL LETTER O WITH LOW RING INSIDE]
						case '\uA74B': 
						// ê�‹  [LATIN SMALL LETTER O WITH LONG STROKE OVERLAY]
						case '\uA74D': 
						// ê��  [LATIN SMALL LETTER O WITH LOOP]
						case '\uFF4F':  // ï½�  [FULLWIDTH LATIN SMALL LETTER O]
							output[outputPos++] = 'o';
							break;
						
						case '\u0152': 
						// Å’  [LATIN CAPITAL LIGATURE OE]
						case '\u0276':  // É¶  [LATIN LETTER SMALL CAPITAL OE]
							output[outputPos++] = 'O';
							output[outputPos++] = 'E';
							break;
						
						case '\uA74E':  // ê�Ž  [LATIN CAPITAL LETTER OO]
							output[outputPos++] = 'O';
							output[outputPos++] = 'O';
							break;
						
						case '\u0222': 
						// È¢  http://en.wikipedia.org/wiki/OU  [LATIN CAPITAL LETTER OU]
						case '\u1D15':  // á´•  [LATIN LETTER SMALL CAPITAL OU]
							output[outputPos++] = 'O';
							output[outputPos++] = 'U';
							break;
						
						case '\u24AA':  // â’ª  [PARENTHESIZED LATIN SMALL LETTER O]
							output[outputPos++] = '(';
							output[outputPos++] = 'o';
							output[outputPos++] = ')';
							break;
						
						case '\u0153': 
						// Å“  [LATIN SMALL LIGATURE OE]
						case '\u1D14':  // á´�?  [LATIN SMALL LETTER TURNED OE]
							output[outputPos++] = 'o';
							output[outputPos++] = 'e';
							break;
						
						case '\uA74F':  // ê��  [LATIN SMALL LETTER OO]
							output[outputPos++] = 'o';
							output[outputPos++] = 'o';
							break;
						
						case '\u0223':  // È£  http://en.wikipedia.org/wiki/OU  [LATIN SMALL LETTER OU]
							output[outputPos++] = 'o';
							output[outputPos++] = 'u';
							break;
						
						case '\u01A4': 
						// Æ¤  [LATIN CAPITAL LETTER P WITH HOOK]
						case '\u1D18': 
						// á´˜  [LATIN LETTER SMALL CAPITAL P]
						case '\u1E54': 
						// á¹�?  [LATIN CAPITAL LETTER P WITH ACUTE]
						case '\u1E56': 
						// á¹–  [LATIN CAPITAL LETTER P WITH DOT ABOVE]
						case '\u24C5': 
						// â“…  [CIRCLED LATIN CAPITAL LETTER P]
						case '\u2C63': 
						// â±£  [LATIN CAPITAL LETTER P WITH STROKE]
						case '\uA750': 
						// ê��  [LATIN CAPITAL LETTER P WITH STROKE THROUGH DESCENDER]
						case '\uA752': 
						// ê�’  [LATIN CAPITAL LETTER P WITH FLOURISH]
						case '\uA754': 
						// ê��?  [LATIN CAPITAL LETTER P WITH SQUIRREL TAIL]
						case '\uFF30':  // ï¼°  [FULLWIDTH LATIN CAPITAL LETTER P]
							output[outputPos++] = 'P';
							break;
						
						case '\u01A5': 
						// Æ¥  [LATIN SMALL LETTER P WITH HOOK]
						case '\u1D71': 
						// áµ±  [LATIN SMALL LETTER P WITH MIDDLE TILDE]
						case '\u1D7D': 
						// áµ½  [LATIN SMALL LETTER P WITH STROKE]
						case '\u1D88': 
						// á¶ˆ  [LATIN SMALL LETTER P WITH PALATAL HOOK]
						case '\u1E55': 
						// á¹•  [LATIN SMALL LETTER P WITH ACUTE]
						case '\u1E57': 
						// á¹—  [LATIN SMALL LETTER P WITH DOT ABOVE]
						case '\u24DF': 
						// â“Ÿ  [CIRCLED LATIN SMALL LETTER P]
						case '\uA751': 
						// ê�‘  [LATIN SMALL LETTER P WITH STROKE THROUGH DESCENDER]
						case '\uA753': 
						// ê�“  [LATIN SMALL LETTER P WITH FLOURISH]
						case '\uA755': 
						// ê�•  [LATIN SMALL LETTER P WITH SQUIRREL TAIL]
						case '\uA7FC': 
						// êŸ¼  [LATIN EPIGRAPHIC LETTER REVERSED P]
						case '\uFF50':  // ï½�  [FULLWIDTH LATIN SMALL LETTER P]
							output[outputPos++] = 'p';
							break;
						
						case '\u24AB':  // â’«  [PARENTHESIZED LATIN SMALL LETTER P]
							output[outputPos++] = '(';
							output[outputPos++] = 'p';
							output[outputPos++] = ')';
							break;
						
						case '\u024A': 
						// ÉŠ  [LATIN CAPITAL LETTER SMALL Q WITH HOOK TAIL]
						case '\u24C6': 
						// â“†  [CIRCLED LATIN CAPITAL LETTER Q]
						case '\uA756': 
						// ê�–  [LATIN CAPITAL LETTER Q WITH STROKE THROUGH DESCENDER]
						case '\uA758': 
						// ê�˜  [LATIN CAPITAL LETTER Q WITH DIAGONAL STROKE]
						case '\uFF31':  // ï¼±  [FULLWIDTH LATIN CAPITAL LETTER Q]
							output[outputPos++] = 'Q';
							break;
						
						case '\u0138': 
						// Ä¸  http://en.wikipedia.org/wiki/Kra_(letter)  [LATIN SMALL LETTER KRA]
						case '\u024B': 
						// É‹  [LATIN SMALL LETTER Q WITH HOOK TAIL]
						case '\u02A0': 
						// Ê   [LATIN SMALL LETTER Q WITH HOOK]
						case '\u24E0': 
						// â“   [CIRCLED LATIN SMALL LETTER Q]
						case '\uA757': 
						// ê�—  [LATIN SMALL LETTER Q WITH STROKE THROUGH DESCENDER]
						case '\uA759': 
						// ê�™  [LATIN SMALL LETTER Q WITH DIAGONAL STROKE]
						case '\uFF51':  // ï½‘  [FULLWIDTH LATIN SMALL LETTER Q]
							output[outputPos++] = 'q';
							break;
						
						case '\u24AC':  // â’¬  [PARENTHESIZED LATIN SMALL LETTER Q]
							output[outputPos++] = '(';
							output[outputPos++] = 'q';
							output[outputPos++] = ')';
							break;
						
						case '\u0239':  // È¹  [LATIN SMALL LETTER QP DIGRAPH]
							output[outputPos++] = 'q';
							output[outputPos++] = 'p';
							break;
						
						case '\u0154': 
						// Å�?  [LATIN CAPITAL LETTER R WITH ACUTE]
						case '\u0156': 
						// Å–  [LATIN CAPITAL LETTER R WITH CEDILLA]
						case '\u0158': 
						// Å˜  [LATIN CAPITAL LETTER R WITH CARON]
						case '\u0210': 
						// È’  [LATIN CAPITAL LETTER R WITH DOUBLE GRAVE]
						case '\u0212': 
						// È’  [LATIN CAPITAL LETTER R WITH INVERTED BREVE]
						case '\u024C': 
						// ÉŒ  [LATIN CAPITAL LETTER R WITH STROKE]
						case '\u0280': 
						// Ê€  [LATIN LETTER SMALL CAPITAL R]
						case '\u0281': 
						// Ê�  [LATIN LETTER SMALL CAPITAL INVERTED R]
						case '\u1D19': 
						// á´™  [LATIN LETTER SMALL CAPITAL REVERSED R]
						case '\u1D1A': 
						// á´š  [LATIN LETTER SMALL CAPITAL TURNED R]
						case '\u1E58': 
						// á¹˜  [LATIN CAPITAL LETTER R WITH DOT ABOVE]
						case '\u1E5A': 
						// á¹š  [LATIN CAPITAL LETTER R WITH DOT BELOW]
						case '\u1E5C': 
						// á¹œ  [LATIN CAPITAL LETTER R WITH DOT BELOW AND MACRON]
						case '\u1E5E': 
						// á¹ž  [LATIN CAPITAL LETTER R WITH LINE BELOW]
						case '\u24C7': 
						// â“‡  [CIRCLED LATIN CAPITAL LETTER R]
						case '\u2C64': 
						// â±¤  [LATIN CAPITAL LETTER R WITH TAIL]
						case '\uA75A': 
						// ê�š  [LATIN CAPITAL LETTER R ROTUNDA]
						case '\uA782': 
						// êž‚  [LATIN CAPITAL LETTER INSULAR R]
						case '\uFF32':  // ï¼²  [FULLWIDTH LATIN CAPITAL LETTER R]
							output[outputPos++] = 'R';
							break;
						
						case '\u0155': 
						// Å•  [LATIN SMALL LETTER R WITH ACUTE]
						case '\u0157': 
						// Å—  [LATIN SMALL LETTER R WITH CEDILLA]
						case '\u0159': 
						// Å™  [LATIN SMALL LETTER R WITH CARON]
						case '\u0211': 
						// È‘  [LATIN SMALL LETTER R WITH DOUBLE GRAVE]
						case '\u0213': 
						// È“  [LATIN SMALL LETTER R WITH INVERTED BREVE]
						case '\u024D': 
						// É�  [LATIN SMALL LETTER R WITH STROKE]
						case '\u027C': 
						// É¼  [LATIN SMALL LETTER R WITH LONG LEG]
						case '\u027D': 
						// É½  [LATIN SMALL LETTER R WITH TAIL]
						case '\u027E': 
						// É¾  [LATIN SMALL LETTER R WITH FISHHOOK]
						case '\u027F': 
						// É¿  [LATIN SMALL LETTER REVERSED R WITH FISHHOOK]
						case '\u1D63': 
						// áµ£  [LATIN SUBSCRIPT SMALL LETTER R]
						case '\u1D72': 
						// áµ²  [LATIN SMALL LETTER R WITH MIDDLE TILDE]
						case '\u1D73': 
						// áµ³  [LATIN SMALL LETTER R WITH FISHHOOK AND MIDDLE TILDE]
						case '\u1D89': 
						// á¶‰  [LATIN SMALL LETTER R WITH PALATAL HOOK]
						case '\u1E59': 
						// á¹™  [LATIN SMALL LETTER R WITH DOT ABOVE]
						case '\u1E5B': 
						// á¹›  [LATIN SMALL LETTER R WITH DOT BELOW]
						case '\u1E5D': 
						// á¹�  [LATIN SMALL LETTER R WITH DOT BELOW AND MACRON]
						case '\u1E5F': 
						// á¹Ÿ  [LATIN SMALL LETTER R WITH LINE BELOW]
						case '\u24E1': 
						// â“¡  [CIRCLED LATIN SMALL LETTER R]
						case '\uA75B': 
						// ê�›  [LATIN SMALL LETTER R ROTUNDA]
						case '\uA783': 
						// êžƒ  [LATIN SMALL LETTER INSULAR R]
						case '\uFF52':  // ï½’  [FULLWIDTH LATIN SMALL LETTER R]
							output[outputPos++] = 'r';
							break;
						
						case '\u24AD':  // â’­  [PARENTHESIZED LATIN SMALL LETTER R]
							output[outputPos++] = '(';
							output[outputPos++] = 'r';
							output[outputPos++] = ')';
							break;
						
						case '\u015A': 
						// Åš  [LATIN CAPITAL LETTER S WITH ACUTE]
						case '\u015C': 
						// Åœ  [LATIN CAPITAL LETTER S WITH CIRCUMFLEX]
						case '\u015E': 
						// Åž  [LATIN CAPITAL LETTER S WITH CEDILLA]
						case '\u0160': 
						// Å   [LATIN CAPITAL LETTER S WITH CARON]
						case '\u0218': 
						// È˜  [LATIN CAPITAL LETTER S WITH COMMA BELOW]
						case '\u1E60': 
						// á¹   [LATIN CAPITAL LETTER S WITH DOT ABOVE]
						case '\u1E62': 
						// á¹¢  [LATIN CAPITAL LETTER S WITH DOT BELOW]
						case '\u1E64': 
						// á¹¤  [LATIN CAPITAL LETTER S WITH ACUTE AND DOT ABOVE]
						case '\u1E66': 
						// á¹¦  [LATIN CAPITAL LETTER S WITH CARON AND DOT ABOVE]
						case '\u1E68': 
						// á¹¨  [LATIN CAPITAL LETTER S WITH DOT BELOW AND DOT ABOVE]
						case '\u24C8': 
						// â“ˆ  [CIRCLED LATIN CAPITAL LETTER S]
						case '\uA731': 
						// êœ±  [LATIN LETTER SMALL CAPITAL S]
						case '\uA785': 
						// êž…  [LATIN SMALL LETTER INSULAR S]
						case '\uFF33':  // ï¼³  [FULLWIDTH LATIN CAPITAL LETTER S]
							output[outputPos++] = 'S';
							break;
						
						case '\u015B': 
						// Å›  [LATIN SMALL LETTER S WITH ACUTE]
						case '\u015D': 
						// Å�  [LATIN SMALL LETTER S WITH CIRCUMFLEX]
						case '\u015F': 
						// ÅŸ  [LATIN SMALL LETTER S WITH CEDILLA]
						case '\u0161': 
						// Å¡  [LATIN SMALL LETTER S WITH CARON]
						case '\u017F': 
						// Å¿  http://en.wikipedia.org/wiki/Long_S  [LATIN SMALL LETTER LONG S]
						case '\u0219': 
						// È™  [LATIN SMALL LETTER S WITH COMMA BELOW]
						case '\u023F': 
						// È¿  [LATIN SMALL LETTER S WITH SWASH TAIL]
						case '\u0282': 
						// Ê‚  [LATIN SMALL LETTER S WITH HOOK]
						case '\u1D74': 
						// áµ´  [LATIN SMALL LETTER S WITH MIDDLE TILDE]
						case '\u1D8A': 
						// á¶Š  [LATIN SMALL LETTER S WITH PALATAL HOOK]
						case '\u1E61': 
						// á¹¡  [LATIN SMALL LETTER S WITH DOT ABOVE]
						case '\u1E63': 
						// á¹£  [LATIN SMALL LETTER S WITH DOT BELOW]
						case '\u1E65': 
						// á¹¥  [LATIN SMALL LETTER S WITH ACUTE AND DOT ABOVE]
						case '\u1E67': 
						// á¹§  [LATIN SMALL LETTER S WITH CARON AND DOT ABOVE]
						case '\u1E69': 
						// á¹©  [LATIN SMALL LETTER S WITH DOT BELOW AND DOT ABOVE]
						case '\u1E9C': 
						// áºœ  [LATIN SMALL LETTER LONG S WITH DIAGONAL STROKE]
						case '\u1E9D': 
						// áº�  [LATIN SMALL LETTER LONG S WITH HIGH STROKE]
						case '\u24E2': 
						// â“¢  [CIRCLED LATIN SMALL LETTER S]
						case '\uA784': 
						// êž„  [LATIN CAPITAL LETTER INSULAR S]
						case '\uFF53':  // ï½“  [FULLWIDTH LATIN SMALL LETTER S]
							output[outputPos++] = 's';
							break;
						
						case '\u1E9E':  // áºž  [LATIN CAPITAL LETTER SHARP S]
							output[outputPos++] = 'S';
							output[outputPos++] = 'S';
							break;
						
						case '\u24AE':  // â’®  [PARENTHESIZED LATIN SMALL LETTER S]
							output[outputPos++] = '(';
							output[outputPos++] = 's';
							output[outputPos++] = ')';
							break;
						
						case '\u00DF':  // ÃŸ  [LATIN SMALL LETTER SHARP S]
							output[outputPos++] = 's';
							output[outputPos++] = 's';
							break;
						
						case '\uFB06':  // ï¬†  [LATIN SMALL LIGATURE ST]
							output[outputPos++] = 's';
							output[outputPos++] = 't';
							break;
						
						case '\u0162': 
						// Å¢  [LATIN CAPITAL LETTER T WITH CEDILLA]
						case '\u0164': 
						// Å¤  [LATIN CAPITAL LETTER T WITH CARON]
						case '\u0166': 
						// Å¦  [LATIN CAPITAL LETTER T WITH STROKE]
						case '\u01AC': 
						// Æ¬  [LATIN CAPITAL LETTER T WITH HOOK]
						case '\u01AE': 
						// Æ®  [LATIN CAPITAL LETTER T WITH RETROFLEX HOOK]
						case '\u021A': 
						// Èš  [LATIN CAPITAL LETTER T WITH COMMA BELOW]
						case '\u023E': 
						// È¾  [LATIN CAPITAL LETTER T WITH DIAGONAL STROKE]
						case '\u1D1B': 
						// á´›  [LATIN LETTER SMALL CAPITAL T]
						case '\u1E6A': 
						// á¹ª  [LATIN CAPITAL LETTER T WITH DOT ABOVE]
						case '\u1E6C': 
						// á¹¬  [LATIN CAPITAL LETTER T WITH DOT BELOW]
						case '\u1E6E': 
						// á¹®  [LATIN CAPITAL LETTER T WITH LINE BELOW]
						case '\u1E70': 
						// á¹°  [LATIN CAPITAL LETTER T WITH CIRCUMFLEX BELOW]
						case '\u24C9': 
						// â“‰  [CIRCLED LATIN CAPITAL LETTER T]
						case '\uA786': 
						// êž†  [LATIN CAPITAL LETTER INSULAR T]
						case '\uFF34':  // ï¼´  [FULLWIDTH LATIN CAPITAL LETTER T]
							output[outputPos++] = 'T';
							break;
						
						case '\u0163': 
						// Å£  [LATIN SMALL LETTER T WITH CEDILLA]
						case '\u0165': 
						// Å¥  [LATIN SMALL LETTER T WITH CARON]
						case '\u0167': 
						// Å§  [LATIN SMALL LETTER T WITH STROKE]
						case '\u01AB': 
						// Æ«  [LATIN SMALL LETTER T WITH PALATAL HOOK]
						case '\u01AD': 
						// Æ­  [LATIN SMALL LETTER T WITH HOOK]
						case '\u021B': 
						// È›  [LATIN SMALL LETTER T WITH COMMA BELOW]
						case '\u0236': 
						// È¶  [LATIN SMALL LETTER T WITH CURL]
						case '\u0287': 
						// Ê‡  [LATIN SMALL LETTER TURNED T]
						case '\u0288': 
						// Êˆ  [LATIN SMALL LETTER T WITH RETROFLEX HOOK]
						case '\u1D75': 
						// áµµ  [LATIN SMALL LETTER T WITH MIDDLE TILDE]
						case '\u1E6B': 
						// á¹«  [LATIN SMALL LETTER T WITH DOT ABOVE]
						case '\u1E6D': 
						// á¹­  [LATIN SMALL LETTER T WITH DOT BELOW]
						case '\u1E6F': 
						// á¹¯  [LATIN SMALL LETTER T WITH LINE BELOW]
						case '\u1E71': 
						// á¹±  [LATIN SMALL LETTER T WITH CIRCUMFLEX BELOW]
						case '\u1E97': 
						// áº—  [LATIN SMALL LETTER T WITH DIAERESIS]
						case '\u24E3': 
						// â“£  [CIRCLED LATIN SMALL LETTER T]
						case '\u2C66': 
						// â±¦  [LATIN SMALL LETTER T WITH DIAGONAL STROKE]
						case '\uFF54':  // ï½�?  [FULLWIDTH LATIN SMALL LETTER T]
							output[outputPos++] = 't';
							break;
						
						case '\u00DE': 
						// Ãž  [LATIN CAPITAL LETTER THORN]
						case '\uA766':  // ê�¦  [LATIN CAPITAL LETTER THORN WITH STROKE THROUGH DESCENDER]
							output[outputPos++] = 'T';
							output[outputPos++] = 'H';
							break;
						
						case '\uA728':  // êœ¨  [LATIN CAPITAL LETTER TZ]
							output[outputPos++] = 'T';
							output[outputPos++] = 'Z';
							break;
						
						case '\u24AF':  // â’¯  [PARENTHESIZED LATIN SMALL LETTER T]
							output[outputPos++] = '(';
							output[outputPos++] = 't';
							output[outputPos++] = ')';
							break;
						
						case '\u02A8':  // Ê¨  [LATIN SMALL LETTER TC DIGRAPH WITH CURL]
							output[outputPos++] = 't';
							output[outputPos++] = 'c';
							break;
						
						case '\u00FE': 
						// Ã¾  [LATIN SMALL LETTER THORN]
						case '\u1D7A': 
						// áµº  [LATIN SMALL LETTER TH WITH STRIKETHROUGH]
						case '\uA767':  // ê�§  [LATIN SMALL LETTER THORN WITH STROKE THROUGH DESCENDER]
							output[outputPos++] = 't';
							output[outputPos++] = 'h';
							break;
						
						case '\u02A6':  // Ê¦  [LATIN SMALL LETTER TS DIGRAPH]
							output[outputPos++] = 't';
							output[outputPos++] = 's';
							break;
						
						case '\uA729':  // êœ©  [LATIN SMALL LETTER TZ]
							output[outputPos++] = 't';
							output[outputPos++] = 'z';
							break;
						
						case '\u00D9': 
						// Ã™  [LATIN CAPITAL LETTER U WITH GRAVE]
						case '\u00DA': 
						// Ãš  [LATIN CAPITAL LETTER U WITH ACUTE]
						case '\u00DB': 
						// Ã›  [LATIN CAPITAL LETTER U WITH CIRCUMFLEX]
						case '\u00DC': 
						// Ãœ  [LATIN CAPITAL LETTER U WITH DIAERESIS]
						case '\u0168': 
						// Å¨  [LATIN CAPITAL LETTER U WITH TILDE]
						case '\u016A': 
						// Åª  [LATIN CAPITAL LETTER U WITH MACRON]
						case '\u016C': 
						// Å¬  [LATIN CAPITAL LETTER U WITH BREVE]
						case '\u016E': 
						// Å®  [LATIN CAPITAL LETTER U WITH RING ABOVE]
						case '\u0170': 
						// Å°  [LATIN CAPITAL LETTER U WITH DOUBLE ACUTE]
						case '\u0172': 
						// Å²  [LATIN CAPITAL LETTER U WITH OGONEK]
						case '\u01AF': 
						// Æ¯  [LATIN CAPITAL LETTER U WITH HORN]
						case '\u01D3': 
						// Ç“  [LATIN CAPITAL LETTER U WITH CARON]
						case '\u01D5': 
						// Ç•  [LATIN CAPITAL LETTER U WITH DIAERESIS AND MACRON]
						case '\u01D7': 
						// Ç—  [LATIN CAPITAL LETTER U WITH DIAERESIS AND ACUTE]
						case '\u01D9': 
						// Ç™  [LATIN CAPITAL LETTER U WITH DIAERESIS AND CARON]
						case '\u01DB': 
						// Ç›  [LATIN CAPITAL LETTER U WITH DIAERESIS AND GRAVE]
						case '\u0214': 
						// È�?  [LATIN CAPITAL LETTER U WITH DOUBLE GRAVE]
						case '\u0216': 
						// È–  [LATIN CAPITAL LETTER U WITH INVERTED BREVE]
						case '\u0244': 
						// É„  [LATIN CAPITAL LETTER U BAR]
						case '\u1D1C': 
						// á´œ  [LATIN LETTER SMALL CAPITAL U]
						case '\u1D7E': 
						// áµ¾  [LATIN SMALL CAPITAL LETTER U WITH STROKE]
						case '\u1E72': 
						// á¹²  [LATIN CAPITAL LETTER U WITH DIAERESIS BELOW]
						case '\u1E74': 
						// á¹´  [LATIN CAPITAL LETTER U WITH TILDE BELOW]
						case '\u1E76': 
						// á¹¶  [LATIN CAPITAL LETTER U WITH CIRCUMFLEX BELOW]
						case '\u1E78': 
						// á¹¸  [LATIN CAPITAL LETTER U WITH TILDE AND ACUTE]
						case '\u1E7A': 
						// á¹º  [LATIN CAPITAL LETTER U WITH MACRON AND DIAERESIS]
						case '\u1EE4': 
						// á»¤  [LATIN CAPITAL LETTER U WITH DOT BELOW]
						case '\u1EE6': 
						// á»¦  [LATIN CAPITAL LETTER U WITH HOOK ABOVE]
						case '\u1EE8': 
						// á»¨  [LATIN CAPITAL LETTER U WITH HORN AND ACUTE]
						case '\u1EEA': 
						// á»ª  [LATIN CAPITAL LETTER U WITH HORN AND GRAVE]
						case '\u1EEC': 
						// á»¬  [LATIN CAPITAL LETTER U WITH HORN AND HOOK ABOVE]
						case '\u1EEE': 
						// á»®  [LATIN CAPITAL LETTER U WITH HORN AND TILDE]
						case '\u1EF0': 
						// á»°  [LATIN CAPITAL LETTER U WITH HORN AND DOT BELOW]
						case '\u24CA': 
						// â“Š  [CIRCLED LATIN CAPITAL LETTER U]
						case '\uFF35':  // ï¼µ  [FULLWIDTH LATIN CAPITAL LETTER U]
							output[outputPos++] = 'U';
							break;
						
						case '\u00F9': 
						// Ã¹  [LATIN SMALL LETTER U WITH GRAVE]
						case '\u00FA': 
						// Ãº  [LATIN SMALL LETTER U WITH ACUTE]
						case '\u00FB': 
						// Ã»  [LATIN SMALL LETTER U WITH CIRCUMFLEX]
						case '\u00FC': 
						// Ã¼  [LATIN SMALL LETTER U WITH DIAERESIS]
						case '\u0169': 
						// Å©  [LATIN SMALL LETTER U WITH TILDE]
						case '\u016B': 
						// Å«  [LATIN SMALL LETTER U WITH MACRON]
						case '\u016D': 
						// Å­  [LATIN SMALL LETTER U WITH BREVE]
						case '\u016F': 
						// Å¯  [LATIN SMALL LETTER U WITH RING ABOVE]
						case '\u0171': 
						// Å±  [LATIN SMALL LETTER U WITH DOUBLE ACUTE]
						case '\u0173': 
						// Å³  [LATIN SMALL LETTER U WITH OGONEK]
						case '\u01B0': 
						// Æ°  [LATIN SMALL LETTER U WITH HORN]
						case '\u01D4': 
						// Ç�?  [LATIN SMALL LETTER U WITH CARON]
						case '\u01D6': 
						// Ç–  [LATIN SMALL LETTER U WITH DIAERESIS AND MACRON]
						case '\u01D8': 
						// Ç˜  [LATIN SMALL LETTER U WITH DIAERESIS AND ACUTE]
						case '\u01DA': 
						// Çš  [LATIN SMALL LETTER U WITH DIAERESIS AND CARON]
						case '\u01DC': 
						// Çœ  [LATIN SMALL LETTER U WITH DIAERESIS AND GRAVE]
						case '\u0215': 
						// È•  [LATIN SMALL LETTER U WITH DOUBLE GRAVE]
						case '\u0217': 
						// È—  [LATIN SMALL LETTER U WITH INVERTED BREVE]
						case '\u0289': 
						// Ê‰  [LATIN SMALL LETTER U BAR]
						case '\u1D64': 
						// áµ¤  [LATIN SUBSCRIPT SMALL LETTER U]
						case '\u1D99': 
						// á¶™  [LATIN SMALL LETTER U WITH RETROFLEX HOOK]
						case '\u1E73': 
						// á¹³  [LATIN SMALL LETTER U WITH DIAERESIS BELOW]
						case '\u1E75': 
						// á¹µ  [LATIN SMALL LETTER U WITH TILDE BELOW]
						case '\u1E77': 
						// á¹·  [LATIN SMALL LETTER U WITH CIRCUMFLEX BELOW]
						case '\u1E79': 
						// á¹¹  [LATIN SMALL LETTER U WITH TILDE AND ACUTE]
						case '\u1E7B': 
						// á¹»  [LATIN SMALL LETTER U WITH MACRON AND DIAERESIS]
						case '\u1EE5': 
						// á»¥  [LATIN SMALL LETTER U WITH DOT BELOW]
						case '\u1EE7': 
						// á»§  [LATIN SMALL LETTER U WITH HOOK ABOVE]
						case '\u1EE9': 
						// á»©  [LATIN SMALL LETTER U WITH HORN AND ACUTE]
						case '\u1EEB': 
						// á»«  [LATIN SMALL LETTER U WITH HORN AND GRAVE]
						case '\u1EED': 
						// á»­  [LATIN SMALL LETTER U WITH HORN AND HOOK ABOVE]
						case '\u1EEF': 
						// á»¯  [LATIN SMALL LETTER U WITH HORN AND TILDE]
						case '\u1EF1': 
						// á»±  [LATIN SMALL LETTER U WITH HORN AND DOT BELOW]
						case '\u24E4': 
						// â“¤  [CIRCLED LATIN SMALL LETTER U]
						case '\uFF55':  // ï½•  [FULLWIDTH LATIN SMALL LETTER U]
							output[outputPos++] = 'u';
							break;
						
						case '\u24B0':  // â’°  [PARENTHESIZED LATIN SMALL LETTER U]
							output[outputPos++] = '(';
							output[outputPos++] = 'u';
							output[outputPos++] = ')';
							break;
						
						case '\u1D6B':  // áµ«  [LATIN SMALL LETTER UE]
							output[outputPos++] = 'u';
							output[outputPos++] = 'e';
							break;
						
						case '\u01B2': 
						// Æ²  [LATIN CAPITAL LETTER V WITH HOOK]
						case '\u0245': 
						// É…  [LATIN CAPITAL LETTER TURNED V]
						case '\u1D20': 
						// á´   [LATIN LETTER SMALL CAPITAL V]
						case '\u1E7C': 
						// á¹¼  [LATIN CAPITAL LETTER V WITH TILDE]
						case '\u1E7E': 
						// á¹¾  [LATIN CAPITAL LETTER V WITH DOT BELOW]
						case '\u1EFC': 
						// á»¼  [LATIN CAPITAL LETTER MIDDLE-WELSH V]
						case '\u24CB': 
						// â“‹  [CIRCLED LATIN CAPITAL LETTER V]
						case '\uA75E': 
						// ê�ž  [LATIN CAPITAL LETTER V WITH DIAGONAL STROKE]
						case '\uA768': 
						// ê�¨  [LATIN CAPITAL LETTER VEND]
						case '\uFF36':  // ï¼¶  [FULLWIDTH LATIN CAPITAL LETTER V]
							output[outputPos++] = 'V';
							break;
						
						case '\u028B': 
						// Ê‹  [LATIN SMALL LETTER V WITH HOOK]
						case '\u028C': 
						// ÊŒ  [LATIN SMALL LETTER TURNED V]
						case '\u1D65': 
						// áµ¥  [LATIN SUBSCRIPT SMALL LETTER V]
						case '\u1D8C': 
						// á¶Œ  [LATIN SMALL LETTER V WITH PALATAL HOOK]
						case '\u1E7D': 
						// á¹½  [LATIN SMALL LETTER V WITH TILDE]
						case '\u1E7F': 
						// á¹¿  [LATIN SMALL LETTER V WITH DOT BELOW]
						case '\u24E5': 
						// â“¥  [CIRCLED LATIN SMALL LETTER V]
						case '\u2C71': 
						// â±±  [LATIN SMALL LETTER V WITH RIGHT HOOK]
						case '\u2C74': 
						// â±´  [LATIN SMALL LETTER V WITH CURL]
						case '\uA75F': 
						// ê�Ÿ  [LATIN SMALL LETTER V WITH DIAGONAL STROKE]
						case '\uFF56':  // ï½–  [FULLWIDTH LATIN SMALL LETTER V]
							output[outputPos++] = 'v';
							break;
						
						case '\uA760':  // ê�   [LATIN CAPITAL LETTER VY]
							output[outputPos++] = 'V';
							output[outputPos++] = 'Y';
							break;
						
						case '\u24B1':  // â’±  [PARENTHESIZED LATIN SMALL LETTER V]
							output[outputPos++] = '(';
							output[outputPos++] = 'v';
							output[outputPos++] = ')';
							break;
						
						case '\uA761':  // ê�¡  [LATIN SMALL LETTER VY]
							output[outputPos++] = 'v';
							output[outputPos++] = 'y';
							break;
						
						case '\u0174': 
						// Å´  [LATIN CAPITAL LETTER W WITH CIRCUMFLEX]
						case '\u01F7': 
						// Ç·  http://en.wikipedia.org/wiki/Wynn  [LATIN CAPITAL LETTER WYNN]
						case '\u1D21': 
						// á´¡  [LATIN LETTER SMALL CAPITAL W]
						case '\u1E80': 
						// áº€  [LATIN CAPITAL LETTER W WITH GRAVE]
						case '\u1E82': 
						// áº‚  [LATIN CAPITAL LETTER W WITH ACUTE]
						case '\u1E84': 
						// áº„  [LATIN CAPITAL LETTER W WITH DIAERESIS]
						case '\u1E86': 
						// áº†  [LATIN CAPITAL LETTER W WITH DOT ABOVE]
						case '\u1E88': 
						// áºˆ  [LATIN CAPITAL LETTER W WITH DOT BELOW]
						case '\u24CC': 
						// â“Œ  [CIRCLED LATIN CAPITAL LETTER W]
						case '\u2C72': 
						// â±²  [LATIN CAPITAL LETTER W WITH HOOK]
						case '\uFF37':  // ï¼·  [FULLWIDTH LATIN CAPITAL LETTER W]
							output[outputPos++] = 'W';
							break;
						
						case '\u0175': 
						// Åµ  [LATIN SMALL LETTER W WITH CIRCUMFLEX]
						case '\u01BF': 
						// Æ¿  http://en.wikipedia.org/wiki/Wynn  [LATIN LETTER WYNN]
						case '\u028D': 
						// Ê�  [LATIN SMALL LETTER TURNED W]
						case '\u1E81': 
						// áº�  [LATIN SMALL LETTER W WITH GRAVE]
						case '\u1E83': 
						// áºƒ  [LATIN SMALL LETTER W WITH ACUTE]
						case '\u1E85': 
						// áº…  [LATIN SMALL LETTER W WITH DIAERESIS]
						case '\u1E87': 
						// áº‡  [LATIN SMALL LETTER W WITH DOT ABOVE]
						case '\u1E89': 
						// áº‰  [LATIN SMALL LETTER W WITH DOT BELOW]
						case '\u1E98': 
						// áº˜  [LATIN SMALL LETTER W WITH RING ABOVE]
						case '\u24E6': 
						// â“¦  [CIRCLED LATIN SMALL LETTER W]
						case '\u2C73': 
						// â±³  [LATIN SMALL LETTER W WITH HOOK]
						case '\uFF57':  // ï½—  [FULLWIDTH LATIN SMALL LETTER W]
							output[outputPos++] = 'w';
							break;
						
						case '\u24B2':  // â’²  [PARENTHESIZED LATIN SMALL LETTER W]
							output[outputPos++] = '(';
							output[outputPos++] = 'w';
							output[outputPos++] = ')';
							break;
						
						case '\u1E8A': 
						// áºŠ  [LATIN CAPITAL LETTER X WITH DOT ABOVE]
						case '\u1E8C': 
						// áºŒ  [LATIN CAPITAL LETTER X WITH DIAERESIS]
						case '\u24CD': 
						// â“�  [CIRCLED LATIN CAPITAL LETTER X]
						case '\uFF38':  // ï¼¸  [FULLWIDTH LATIN CAPITAL LETTER X]
							output[outputPos++] = 'X';
							break;
						
						case '\u1D8D': 
						// á¶�  [LATIN SMALL LETTER X WITH PALATAL HOOK]
						case '\u1E8B': 
						// áº‹  [LATIN SMALL LETTER X WITH DOT ABOVE]
						case '\u1E8D': 
						// áº�  [LATIN SMALL LETTER X WITH DIAERESIS]
						case '\u2093': 
						// â‚“  [LATIN SUBSCRIPT SMALL LETTER X]
						case '\u24E7': 
						// â“§  [CIRCLED LATIN SMALL LETTER X]
						case '\uFF58':  // ï½˜  [FULLWIDTH LATIN SMALL LETTER X]
							output[outputPos++] = 'x';
							break;
						
						case '\u24B3':  // â’³  [PARENTHESIZED LATIN SMALL LETTER X]
							output[outputPos++] = '(';
							output[outputPos++] = 'x';
							output[outputPos++] = ')';
							break;
						
						case '\u00DD': 
						// Ã�  [LATIN CAPITAL LETTER Y WITH ACUTE]
						case '\u0176': 
						// Å¶  [LATIN CAPITAL LETTER Y WITH CIRCUMFLEX]
						case '\u0178': 
						// Å¸  [LATIN CAPITAL LETTER Y WITH DIAERESIS]
						case '\u01B3': 
						// Æ³  [LATIN CAPITAL LETTER Y WITH HOOK]
						case '\u0232': 
						// È²  [LATIN CAPITAL LETTER Y WITH MACRON]
						case '\u024E': 
						// ÉŽ  [LATIN CAPITAL LETTER Y WITH STROKE]
						case '\u028F': 
						// Ê�  [LATIN LETTER SMALL CAPITAL Y]
						case '\u1E8E': 
						// áºŽ  [LATIN CAPITAL LETTER Y WITH DOT ABOVE]
						case '\u1EF2': 
						// á»²  [LATIN CAPITAL LETTER Y WITH GRAVE]
						case '\u1EF4': 
						// á»´  [LATIN CAPITAL LETTER Y WITH DOT BELOW]
						case '\u1EF6': 
						// á»¶  [LATIN CAPITAL LETTER Y WITH HOOK ABOVE]
						case '\u1EF8': 
						// á»¸  [LATIN CAPITAL LETTER Y WITH TILDE]
						case '\u1EFE': 
						// á»¾  [LATIN CAPITAL LETTER Y WITH LOOP]
						case '\u24CE': 
						// â“Ž  [CIRCLED LATIN CAPITAL LETTER Y]
						case '\uFF39':  // ï¼¹  [FULLWIDTH LATIN CAPITAL LETTER Y]
							output[outputPos++] = 'Y';
							break;
						
						case '\u00FD': 
						// Ã½  [LATIN SMALL LETTER Y WITH ACUTE]
						case '\u00FF': 
						// Ã¿  [LATIN SMALL LETTER Y WITH DIAERESIS]
						case '\u0177': 
						// Å·  [LATIN SMALL LETTER Y WITH CIRCUMFLEX]
						case '\u01B4': 
						// Æ´  [LATIN SMALL LETTER Y WITH HOOK]
						case '\u0233': 
						// È³  [LATIN SMALL LETTER Y WITH MACRON]
						case '\u024F': 
						// É�  [LATIN SMALL LETTER Y WITH STROKE]
						case '\u028E': 
						// ÊŽ  [LATIN SMALL LETTER TURNED Y]
						case '\u1E8F': 
						// áº�  [LATIN SMALL LETTER Y WITH DOT ABOVE]
						case '\u1E99': 
						// áº™  [LATIN SMALL LETTER Y WITH RING ABOVE]
						case '\u1EF3': 
						// á»³  [LATIN SMALL LETTER Y WITH GRAVE]
						case '\u1EF5': 
						// á»µ  [LATIN SMALL LETTER Y WITH DOT BELOW]
						case '\u1EF7': 
						// á»·  [LATIN SMALL LETTER Y WITH HOOK ABOVE]
						case '\u1EF9': 
						// á»¹  [LATIN SMALL LETTER Y WITH TILDE]
						case '\u1EFF': 
						// á»¿  [LATIN SMALL LETTER Y WITH LOOP]
						case '\u24E8': 
						// â“¨  [CIRCLED LATIN SMALL LETTER Y]
						case '\uFF59':  // ï½™  [FULLWIDTH LATIN SMALL LETTER Y]
							output[outputPos++] = 'y';
							break;
						
						case '\u24B4':  // â’´  [PARENTHESIZED LATIN SMALL LETTER Y]
							output[outputPos++] = '(';
							output[outputPos++] = 'y';
							output[outputPos++] = ')';
							break;
						
						case '\u0179': 
						// Å¹  [LATIN CAPITAL LETTER Z WITH ACUTE]
						case '\u017B': 
						// Å»  [LATIN CAPITAL LETTER Z WITH DOT ABOVE]
						case '\u017D': 
						// Å½  [LATIN CAPITAL LETTER Z WITH CARON]
						case '\u01B5': 
						// Æµ  [LATIN CAPITAL LETTER Z WITH STROKE]
						case '\u021C': 
						// Èœ  http://en.wikipedia.org/wiki/Yogh  [LATIN CAPITAL LETTER YOGH]
						case '\u0224': 
						// È¤  [LATIN CAPITAL LETTER Z WITH HOOK]
						case '\u1D22': 
						// á´¢  [LATIN LETTER SMALL CAPITAL Z]
						case '\u1E90': 
						// áº�  [LATIN CAPITAL LETTER Z WITH CIRCUMFLEX]
						case '\u1E92': 
						// áº’  [LATIN CAPITAL LETTER Z WITH DOT BELOW]
						case '\u1E94': 
						// áº�?  [LATIN CAPITAL LETTER Z WITH LINE BELOW]
						case '\u24CF': 
						// â“�  [CIRCLED LATIN CAPITAL LETTER Z]
						case '\u2C6B': 
						// â±«  [LATIN CAPITAL LETTER Z WITH DESCENDER]
						case '\uA762': 
						// ê�¢  [LATIN CAPITAL LETTER VISIGOTHIC Z]
						case '\uFF3A':  // ï¼º  [FULLWIDTH LATIN CAPITAL LETTER Z]
							output[outputPos++] = 'Z';
							break;
						
						case '\u017A': 
						// Åº  [LATIN SMALL LETTER Z WITH ACUTE]
						case '\u017C': 
						// Å¼  [LATIN SMALL LETTER Z WITH DOT ABOVE]
						case '\u017E': 
						// Å¾  [LATIN SMALL LETTER Z WITH CARON]
						case '\u01B6': 
						// Æ¶  [LATIN SMALL LETTER Z WITH STROKE]
						case '\u021D': 
						// È�  http://en.wikipedia.org/wiki/Yogh  [LATIN SMALL LETTER YOGH]
						case '\u0225': 
						// È¥  [LATIN SMALL LETTER Z WITH HOOK]
						case '\u0240': 
						// É€  [LATIN SMALL LETTER Z WITH SWASH TAIL]
						case '\u0290': 
						// Ê�  [LATIN SMALL LETTER Z WITH RETROFLEX HOOK]
						case '\u0291': 
						// Ê‘  [LATIN SMALL LETTER Z WITH CURL]
						case '\u1D76': 
						// áµ¶  [LATIN SMALL LETTER Z WITH MIDDLE TILDE]
						case '\u1D8E': 
						// á¶Ž  [LATIN SMALL LETTER Z WITH PALATAL HOOK]
						case '\u1E91': 
						// áº‘  [LATIN SMALL LETTER Z WITH CIRCUMFLEX]
						case '\u1E93': 
						// áº“  [LATIN SMALL LETTER Z WITH DOT BELOW]
						case '\u1E95': 
						// áº•  [LATIN SMALL LETTER Z WITH LINE BELOW]
						case '\u24E9': 
						// â“©  [CIRCLED LATIN SMALL LETTER Z]
						case '\u2C6C': 
						// â±¬  [LATIN SMALL LETTER Z WITH DESCENDER]
						case '\uA763': 
						// ê�£  [LATIN SMALL LETTER VISIGOTHIC Z]
						case '\uFF5A':  // ï½š  [FULLWIDTH LATIN SMALL LETTER Z]
							output[outputPos++] = 'z';
							break;
						
						case '\u24B5':  // â’µ  [PARENTHESIZED LATIN SMALL LETTER Z]
							output[outputPos++] = '(';
							output[outputPos++] = 'z';
							output[outputPos++] = ')';
							break;
						
						case '\u2070': 
						// â�°  [SUPERSCRIPT ZERO]
						case '\u2080': 
						// â‚€  [SUBSCRIPT ZERO]
						case '\u24EA': 
						// â“ª  [CIRCLED DIGIT ZERO]
						case '\u24FF': 
						// â“¿  [NEGATIVE CIRCLED DIGIT ZERO]
						case '\uFF10':  // ï¼�  [FULLWIDTH DIGIT ZERO]
							output[outputPos++] = '0';
							break;
						
						case '\u00B9': 
						// Â¹  [SUPERSCRIPT ONE]
						case '\u2081': 
						// â‚�  [SUBSCRIPT ONE]
						case '\u2460': 
						// â‘   [CIRCLED DIGIT ONE]
						case '\u24F5': 
						// â“µ  [DOUBLE CIRCLED DIGIT ONE]
						case '\u2776': 
						// â�¶  [DINGBAT NEGATIVE CIRCLED DIGIT ONE]
						case '\u2780': 
						// âž€  [DINGBAT CIRCLED SANS-SERIF DIGIT ONE]
						case '\u278A': 
						// âžŠ  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT ONE]
						case '\uFF11':  // ï¼‘  [FULLWIDTH DIGIT ONE]
							output[outputPos++] = '1';
							break;
						
						case '\u2488':  // â’ˆ  [DIGIT ONE FULL STOP]
							output[outputPos++] = '1';
							output[outputPos++] = '.';
							break;
						
						case '\u2474':  // â‘´  [PARENTHESIZED DIGIT ONE]
							output[outputPos++] = '(';
							output[outputPos++] = '1';
							output[outputPos++] = ')';
							break;
						
						case '\u00B2': 
						// Â²  [SUPERSCRIPT TWO]
						case '\u2082': 
						// â‚‚  [SUBSCRIPT TWO]
						case '\u2461': 
						// â‘¡  [CIRCLED DIGIT TWO]
						case '\u24F6': 
						// â“¶  [DOUBLE CIRCLED DIGIT TWO]
						case '\u2777': 
						// â�·  [DINGBAT NEGATIVE CIRCLED DIGIT TWO]
						case '\u2781': 
						// âž�  [DINGBAT CIRCLED SANS-SERIF DIGIT TWO]
						case '\u278B': 
						// âž‹  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT TWO]
						case '\uFF12':  // ï¼’  [FULLWIDTH DIGIT TWO]
							output[outputPos++] = '2';
							break;
						
						case '\u2489':  // â’‰  [DIGIT TWO FULL STOP]
							output[outputPos++] = '2';
							output[outputPos++] = '.';
							break;
						
						case '\u2475':  // â‘µ  [PARENTHESIZED DIGIT TWO]
							output[outputPos++] = '(';
							output[outputPos++] = '2';
							output[outputPos++] = ')';
							break;
						
						case '\u00B3': 
						// Â³  [SUPERSCRIPT THREE]
						case '\u2083': 
						// â‚ƒ  [SUBSCRIPT THREE]
						case '\u2462': 
						// â‘¢  [CIRCLED DIGIT THREE]
						case '\u24F7': 
						// â“·  [DOUBLE CIRCLED DIGIT THREE]
						case '\u2778': 
						// â�¸  [DINGBAT NEGATIVE CIRCLED DIGIT THREE]
						case '\u2782': 
						// âž‚  [DINGBAT CIRCLED SANS-SERIF DIGIT THREE]
						case '\u278C': 
						// âžŒ  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT THREE]
						case '\uFF13':  // ï¼“  [FULLWIDTH DIGIT THREE]
							output[outputPos++] = '3';
							break;
						
						case '\u248A':  // â’Š  [DIGIT THREE FULL STOP]
							output[outputPos++] = '3';
							output[outputPos++] = '.';
							break;
						
						case '\u2476':  // â‘¶  [PARENTHESIZED DIGIT THREE]
							output[outputPos++] = '(';
							output[outputPos++] = '3';
							output[outputPos++] = ')';
							break;
						
						case '\u2074': 
						// â�´  [SUPERSCRIPT FOUR]
						case '\u2084': 
						// â‚„  [SUBSCRIPT FOUR]
						case '\u2463': 
						// â‘£  [CIRCLED DIGIT FOUR]
						case '\u24F8': 
						// â“¸  [DOUBLE CIRCLED DIGIT FOUR]
						case '\u2779': 
						// â�¹  [DINGBAT NEGATIVE CIRCLED DIGIT FOUR]
						case '\u2783': 
						// âžƒ  [DINGBAT CIRCLED SANS-SERIF DIGIT FOUR]
						case '\u278D': 
						// âž�  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT FOUR]
						case '\uFF14':  // ï¼�?  [FULLWIDTH DIGIT FOUR]
							output[outputPos++] = '4';
							break;
						
						case '\u248B':  // â’‹  [DIGIT FOUR FULL STOP]
							output[outputPos++] = '4';
							output[outputPos++] = '.';
							break;
						
						case '\u2477':  // â‘·  [PARENTHESIZED DIGIT FOUR]
							output[outputPos++] = '(';
							output[outputPos++] = '4';
							output[outputPos++] = ')';
							break;
						
						case '\u2075': 
						// â�µ  [SUPERSCRIPT FIVE]
						case '\u2085': 
						// â‚…  [SUBSCRIPT FIVE]
						case '\u2464': 
						// â‘¤  [CIRCLED DIGIT FIVE]
						case '\u24F9': 
						// â“¹  [DOUBLE CIRCLED DIGIT FIVE]
						case '\u277A': 
						// â�º  [DINGBAT NEGATIVE CIRCLED DIGIT FIVE]
						case '\u2784': 
						// âž„  [DINGBAT CIRCLED SANS-SERIF DIGIT FIVE]
						case '\u278E': 
						// âžŽ  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT FIVE]
						case '\uFF15':  // ï¼•  [FULLWIDTH DIGIT FIVE]
							output[outputPos++] = '5';
							break;
						
						case '\u248C':  // â’Œ  [DIGIT FIVE FULL STOP]
							output[outputPos++] = '5';
							output[outputPos++] = '.';
							break;
						
						case '\u2478':  // â‘¸  [PARENTHESIZED DIGIT FIVE]
							output[outputPos++] = '(';
							output[outputPos++] = '5';
							output[outputPos++] = ')';
							break;
						
						case '\u2076': 
						// â�¶  [SUPERSCRIPT SIX]
						case '\u2086': 
						// â‚†  [SUBSCRIPT SIX]
						case '\u2465': 
						// â‘¥  [CIRCLED DIGIT SIX]
						case '\u24FA': 
						// â“º  [DOUBLE CIRCLED DIGIT SIX]
						case '\u277B': 
						// â�»  [DINGBAT NEGATIVE CIRCLED DIGIT SIX]
						case '\u2785': 
						// âž…  [DINGBAT CIRCLED SANS-SERIF DIGIT SIX]
						case '\u278F': 
						// âž�  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT SIX]
						case '\uFF16':  // ï¼–  [FULLWIDTH DIGIT SIX]
							output[outputPos++] = '6';
							break;
						
						case '\u248D':  // â’�  [DIGIT SIX FULL STOP]
							output[outputPos++] = '6';
							output[outputPos++] = '.';
							break;
						
						case '\u2479':  // â‘¹  [PARENTHESIZED DIGIT SIX]
							output[outputPos++] = '(';
							output[outputPos++] = '6';
							output[outputPos++] = ')';
							break;
						
						case '\u2077': 
						// â�·  [SUPERSCRIPT SEVEN]
						case '\u2087': 
						// â‚‡  [SUBSCRIPT SEVEN]
						case '\u2466': 
						// â‘¦  [CIRCLED DIGIT SEVEN]
						case '\u24FB': 
						// â“»  [DOUBLE CIRCLED DIGIT SEVEN]
						case '\u277C': 
						// â�¼  [DINGBAT NEGATIVE CIRCLED DIGIT SEVEN]
						case '\u2786': 
						// âž†  [DINGBAT CIRCLED SANS-SERIF DIGIT SEVEN]
						case '\u2790': 
						// âž�  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT SEVEN]
						case '\uFF17':  // ï¼—  [FULLWIDTH DIGIT SEVEN]
							output[outputPos++] = '7';
							break;
						
						case '\u248E':  // â’Ž  [DIGIT SEVEN FULL STOP]
							output[outputPos++] = '7';
							output[outputPos++] = '.';
							break;
						
						case '\u247A':  // â‘º  [PARENTHESIZED DIGIT SEVEN]
							output[outputPos++] = '(';
							output[outputPos++] = '7';
							output[outputPos++] = ')';
							break;
						
						case '\u2078': 
						// â�¸  [SUPERSCRIPT EIGHT]
						case '\u2088': 
						// â‚ˆ  [SUBSCRIPT EIGHT]
						case '\u2467': 
						// â‘§  [CIRCLED DIGIT EIGHT]
						case '\u24FC': 
						// â“¼  [DOUBLE CIRCLED DIGIT EIGHT]
						case '\u277D': 
						// â�½  [DINGBAT NEGATIVE CIRCLED DIGIT EIGHT]
						case '\u2787': 
						// âž‡  [DINGBAT CIRCLED SANS-SERIF DIGIT EIGHT]
						case '\u2791': 
						// âž‘  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT EIGHT]
						case '\uFF18':  // ï¼˜  [FULLWIDTH DIGIT EIGHT]
							output[outputPos++] = '8';
							break;
						
						case '\u248F':  // â’�  [DIGIT EIGHT FULL STOP]
							output[outputPos++] = '8';
							output[outputPos++] = '.';
							break;
						
						case '\u247B':  // â‘»  [PARENTHESIZED DIGIT EIGHT]
							output[outputPos++] = '(';
							output[outputPos++] = '8';
							output[outputPos++] = ')';
							break;
						
						case '\u2079': 
						// â�¹  [SUPERSCRIPT NINE]
						case '\u2089': 
						// â‚‰  [SUBSCRIPT NINE]
						case '\u2468': 
						// â‘¨  [CIRCLED DIGIT NINE]
						case '\u24FD': 
						// â“½  [DOUBLE CIRCLED DIGIT NINE]
						case '\u277E': 
						// â�¾  [DINGBAT NEGATIVE CIRCLED DIGIT NINE]
						case '\u2788': 
						// âžˆ  [DINGBAT CIRCLED SANS-SERIF DIGIT NINE]
						case '\u2792': 
						// âž’  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT NINE]
						case '\uFF19':  // ï¼™  [FULLWIDTH DIGIT NINE]
							output[outputPos++] = '9';
							break;
						
						case '\u2490':  // â’�  [DIGIT NINE FULL STOP]
							output[outputPos++] = '9';
							output[outputPos++] = '.';
							break;
						
						case '\u247C':  // â‘¼  [PARENTHESIZED DIGIT NINE]
							output[outputPos++] = '(';
							output[outputPos++] = '9';
							output[outputPos++] = ')';
							break;
						
						case '\u2469': 
						// â‘©  [CIRCLED NUMBER TEN]
						case '\u24FE': 
						// â“¾  [DOUBLE CIRCLED NUMBER TEN]
						case '\u277F': 
						// â�¿  [DINGBAT NEGATIVE CIRCLED NUMBER TEN]
						case '\u2789': 
						// âž‰  [DINGBAT CIRCLED SANS-SERIF NUMBER TEN]
						case '\u2793':  // âž“  [DINGBAT NEGATIVE CIRCLED SANS-SERIF NUMBER TEN]
							output[outputPos++] = '1';
							output[outputPos++] = '0';
							break;
						
						case '\u2491':  // â’‘  [NUMBER TEN FULL STOP]
							output[outputPos++] = '1';
							output[outputPos++] = '0';
							output[outputPos++] = '.';
							break;
						
						case '\u247D':  // â‘½  [PARENTHESIZED NUMBER TEN]
							output[outputPos++] = '(';
							output[outputPos++] = '1';
							output[outputPos++] = '0';
							output[outputPos++] = ')';
							break;
						
						case '\u246A': 
						// â‘ª  [CIRCLED NUMBER ELEVEN]
						case '\u24EB':  // â“«  [NEGATIVE CIRCLED NUMBER ELEVEN]
							output[outputPos++] = '1';
							output[outputPos++] = '1';
							break;
						
						case '\u2492':  // â’’  [NUMBER ELEVEN FULL STOP]
							output[outputPos++] = '1';
							output[outputPos++] = '1';
							output[outputPos++] = '.';
							break;
						
						case '\u247E':  // â‘¾  [PARENTHESIZED NUMBER ELEVEN]
							output[outputPos++] = '(';
							output[outputPos++] = '1';
							output[outputPos++] = '1';
							output[outputPos++] = ')';
							break;
						
						case '\u246B': 
						// â‘«  [CIRCLED NUMBER TWELVE]
						case '\u24EC':  // â“¬  [NEGATIVE CIRCLED NUMBER TWELVE]
							output[outputPos++] = '1';
							output[outputPos++] = '2';
							break;
						
						case '\u2493':  // â’“  [NUMBER TWELVE FULL STOP]
							output[outputPos++] = '1';
							output[outputPos++] = '2';
							output[outputPos++] = '.';
							break;
						
						case '\u247F':  // â‘¿  [PARENTHESIZED NUMBER TWELVE]
							output[outputPos++] = '(';
							output[outputPos++] = '1';
							output[outputPos++] = '2';
							output[outputPos++] = ')';
							break;
						
						case '\u246C': 
						// â‘¬  [CIRCLED NUMBER THIRTEEN]
						case '\u24ED':  // â“­  [NEGATIVE CIRCLED NUMBER THIRTEEN]
							output[outputPos++] = '1';
							output[outputPos++] = '3';
							break;
						
						case '\u2494':  // â’�?  [NUMBER THIRTEEN FULL STOP]
							output[outputPos++] = '1';
							output[outputPos++] = '3';
							output[outputPos++] = '.';
							break;
						
						case '\u2480':  // â’€  [PARENTHESIZED NUMBER THIRTEEN]
							output[outputPos++] = '(';
							output[outputPos++] = '1';
							output[outputPos++] = '3';
							output[outputPos++] = ')';
							break;
						
						case '\u246D': 
						// â‘­  [CIRCLED NUMBER FOURTEEN]
						case '\u24EE':  // â“®  [NEGATIVE CIRCLED NUMBER FOURTEEN]
							output[outputPos++] = '1';
							output[outputPos++] = '4';
							break;
						
						case '\u2495':  // â’•  [NUMBER FOURTEEN FULL STOP]
							output[outputPos++] = '1';
							output[outputPos++] = '4';
							output[outputPos++] = '.';
							break;
						
						case '\u2481':  // â’�  [PARENTHESIZED NUMBER FOURTEEN]
							output[outputPos++] = '(';
							output[outputPos++] = '1';
							output[outputPos++] = '4';
							output[outputPos++] = ')';
							break;
						
						case '\u246E': 
						// â‘®  [CIRCLED NUMBER FIFTEEN]
						case '\u24EF':  // â“¯  [NEGATIVE CIRCLED NUMBER FIFTEEN]
							output[outputPos++] = '1';
							output[outputPos++] = '5';
							break;
						
						case '\u2496':  // â’–  [NUMBER FIFTEEN FULL STOP]
							output[outputPos++] = '1';
							output[outputPos++] = '5';
							output[outputPos++] = '.';
							break;
						
						case '\u2482':  // â’‚  [PARENTHESIZED NUMBER FIFTEEN]
							output[outputPos++] = '(';
							output[outputPos++] = '1';
							output[outputPos++] = '5';
							output[outputPos++] = ')';
							break;
						
						case '\u246F': 
						// â‘¯  [CIRCLED NUMBER SIXTEEN]
						case '\u24F0':  // â“°  [NEGATIVE CIRCLED NUMBER SIXTEEN]
							output[outputPos++] = '1';
							output[outputPos++] = '6';
							break;
						
						case '\u2497':  // â’—  [NUMBER SIXTEEN FULL STOP]
							output[outputPos++] = '1';
							output[outputPos++] = '6';
							output[outputPos++] = '.';
							break;
						
						case '\u2483':  // â’ƒ  [PARENTHESIZED NUMBER SIXTEEN]
							output[outputPos++] = '(';
							output[outputPos++] = '1';
							output[outputPos++] = '6';
							output[outputPos++] = ')';
							break;
						
						case '\u2470': 
						// â‘°  [CIRCLED NUMBER SEVENTEEN]
						case '\u24F1':  // â“±  [NEGATIVE CIRCLED NUMBER SEVENTEEN]
							output[outputPos++] = '1';
							output[outputPos++] = '7';
							break;
						
						case '\u2498':  // â’˜  [NUMBER SEVENTEEN FULL STOP]
							output[outputPos++] = '1';
							output[outputPos++] = '7';
							output[outputPos++] = '.';
							break;
						
						case '\u2484':  // â’„  [PARENTHESIZED NUMBER SEVENTEEN]
							output[outputPos++] = '(';
							output[outputPos++] = '1';
							output[outputPos++] = '7';
							output[outputPos++] = ')';
							break;
						
						case '\u2471': 
						// â‘±  [CIRCLED NUMBER EIGHTEEN]
						case '\u24F2':  // â“²  [NEGATIVE CIRCLED NUMBER EIGHTEEN]
							output[outputPos++] = '1';
							output[outputPos++] = '8';
							break;
						
						case '\u2499':  // â’™  [NUMBER EIGHTEEN FULL STOP]
							output[outputPos++] = '1';
							output[outputPos++] = '8';
							output[outputPos++] = '.';
							break;
						
						case '\u2485':  // â’…  [PARENTHESIZED NUMBER EIGHTEEN]
							output[outputPos++] = '(';
							output[outputPos++] = '1';
							output[outputPos++] = '8';
							output[outputPos++] = ')';
							break;
						
						case '\u2472': 
						// â‘²  [CIRCLED NUMBER NINETEEN]
						case '\u24F3':  // â“³  [NEGATIVE CIRCLED NUMBER NINETEEN]
							output[outputPos++] = '1';
							output[outputPos++] = '9';
							break;
						
						case '\u249A':  // â’š  [NUMBER NINETEEN FULL STOP]
							output[outputPos++] = '1';
							output[outputPos++] = '9';
							output[outputPos++] = '.';
							break;
						
						case '\u2486':  // â’†  [PARENTHESIZED NUMBER NINETEEN]
							output[outputPos++] = '(';
							output[outputPos++] = '1';
							output[outputPos++] = '9';
							output[outputPos++] = ')';
							break;
						
						case '\u2473': 
						// â‘³  [CIRCLED NUMBER TWENTY]
						case '\u24F4':  // â“´  [NEGATIVE CIRCLED NUMBER TWENTY]
							output[outputPos++] = '2';
							output[outputPos++] = '0';
							break;
						
						case '\u249B':  // â’›  [NUMBER TWENTY FULL STOP]
							output[outputPos++] = '2';
							output[outputPos++] = '0';
							output[outputPos++] = '.';
							break;
						
						case '\u2487':  // â’‡  [PARENTHESIZED NUMBER TWENTY]
							output[outputPos++] = '(';
							output[outputPos++] = '2';
							output[outputPos++] = '0';
							output[outputPos++] = ')';
							break;
						
						case '\u00AB': 
						// Â«  [LEFT-POINTING DOUBLE ANGLE QUOTATION MARK]
						case '\u00BB': 
						// Â»  [RIGHT-POINTING DOUBLE ANGLE QUOTATION MARK]
						case '\u201C': 
						// â€œ  [LEFT DOUBLE QUOTATION MARK]
						case '\u201D': 
						// â€�  [RIGHT DOUBLE QUOTATION MARK]
						case '\u201E': 
						// â€ž  [DOUBLE LOW-9 QUOTATION MARK]
						case '\u2033': 
						// â€³  [DOUBLE PRIME]
						case '\u2036': 
						// â€¶  [REVERSED DOUBLE PRIME]
						case '\u275D': 
						// â��  [HEAVY DOUBLE TURNED COMMA QUOTATION MARK ORNAMENT]
						case '\u275E': 
						// â�ž  [HEAVY DOUBLE COMMA QUOTATION MARK ORNAMENT]
						case '\u276E': 
						// â�®  [HEAVY LEFT-POINTING ANGLE QUOTATION MARK ORNAMENT]
						case '\u276F': 
						// â�¯  [HEAVY RIGHT-POINTING ANGLE QUOTATION MARK ORNAMENT]
						case '\uFF02':  // ï¼‚  [FULLWIDTH QUOTATION MARK]
							output[outputPos++] = '"';
							break;
						
						case '\u2018': 
						// â€˜  [LEFT SINGLE QUOTATION MARK]
						case '\u2019': 
						// â€™  [RIGHT SINGLE QUOTATION MARK]
						case '\u201A': 
						// â€š  [SINGLE LOW-9 QUOTATION MARK]
						case '\u201B': 
						// â€›  [SINGLE HIGH-REVERSED-9 QUOTATION MARK]
						case '\u2032': 
						// â€²  [PRIME]
						case '\u2035': 
						// â€µ  [REVERSED PRIME]
						case '\u2039': 
						// â€¹  [SINGLE LEFT-POINTING ANGLE QUOTATION MARK]
						case '\u203A': 
						// â€º  [SINGLE RIGHT-POINTING ANGLE QUOTATION MARK]
						case '\u275B': 
						// â�›  [HEAVY SINGLE TURNED COMMA QUOTATION MARK ORNAMENT]
						case '\u275C': 
						// â�œ  [HEAVY SINGLE COMMA QUOTATION MARK ORNAMENT]
						case '\uFF07':  // ï¼‡  [FULLWIDTH APOSTROPHE]
							output[outputPos++] = '\'';
							break;
						
						case '\u2010': 
						// â€�  [HYPHEN]
						case '\u2011': 
						// â€‘  [NON-BREAKING HYPHEN]
						case '\u2012': 
						// â€’  [FIGURE DASH]
						case '\u2013': 
						// â€“  [EN DASH]
						case '\u2014': 
						// â€�?  [EM DASH]
						case '\u207B': 
						// â�»  [SUPERSCRIPT MINUS]
						case '\u208B': 
						// â‚‹  [SUBSCRIPT MINUS]
						case '\uFF0D':  // ï¼�  [FULLWIDTH HYPHEN-MINUS]
							output[outputPos++] = '-';
							break;
						
						case '\u2045': 
						// â�…  [LEFT SQUARE BRACKET WITH QUILL]
						case '\u2772': 
						// â�²  [LIGHT LEFT TORTOISE SHELL BRACKET ORNAMENT]
						case '\uFF3B':  // ï¼»  [FULLWIDTH LEFT SQUARE BRACKET]
							output[outputPos++] = '[';
							break;
						
						case '\u2046': 
						// â�†  [RIGHT SQUARE BRACKET WITH QUILL]
						case '\u2773': 
						// â�³  [LIGHT RIGHT TORTOISE SHELL BRACKET ORNAMENT]
						case '\uFF3D':  // ï¼½  [FULLWIDTH RIGHT SQUARE BRACKET]
							output[outputPos++] = ']';
							break;
						
						case '\u207D': 
						// â�½  [SUPERSCRIPT LEFT PARENTHESIS]
						case '\u208D': 
						// â‚�  [SUBSCRIPT LEFT PARENTHESIS]
						case '\u2768': 
						// â�¨  [MEDIUM LEFT PARENTHESIS ORNAMENT]
						case '\u276A': 
						// â�ª  [MEDIUM FLATTENED LEFT PARENTHESIS ORNAMENT]
						case '\uFF08':  // ï¼ˆ  [FULLWIDTH LEFT PARENTHESIS]
							output[outputPos++] = '(';
							break;
						
						case '\u2E28':  // â¸¨  [LEFT DOUBLE PARENTHESIS]
							output[outputPos++] = '(';
							output[outputPos++] = '(';
							break;
						
						case '\u207E': 
						// â�¾  [SUPERSCRIPT RIGHT PARENTHESIS]
						case '\u208E': 
						// â‚Ž  [SUBSCRIPT RIGHT PARENTHESIS]
						case '\u2769': 
						// â�©  [MEDIUM RIGHT PARENTHESIS ORNAMENT]
						case '\u276B': 
						// â�«  [MEDIUM FLATTENED RIGHT PARENTHESIS ORNAMENT]
						case '\uFF09':  // ï¼‰  [FULLWIDTH RIGHT PARENTHESIS]
							output[outputPos++] = ')';
							break;
						
						case '\u2E29':  // â¸©  [RIGHT DOUBLE PARENTHESIS]
							output[outputPos++] = ')';
							output[outputPos++] = ')';
							break;
						
						case '\u276C': 
						// â�¬  [MEDIUM LEFT-POINTING ANGLE BRACKET ORNAMENT]
						case '\u2770': 
						// â�°  [HEAVY LEFT-POINTING ANGLE BRACKET ORNAMENT]
						case '\uFF1C':  // ï¼œ  [FULLWIDTH LESS-THAN SIGN]
							output[outputPos++] = '<';
							break;
						
						case '\u276D': 
						// â�­  [MEDIUM RIGHT-POINTING ANGLE BRACKET ORNAMENT]
						case '\u2771': 
						// â�±  [HEAVY RIGHT-POINTING ANGLE BRACKET ORNAMENT]
						case '\uFF1E':  // ï¼ž  [FULLWIDTH GREATER-THAN SIGN]
							output[outputPos++] = '>';
							break;
						
						case '\u2774': 
						// â�´  [MEDIUM LEFT CURLY BRACKET ORNAMENT]
						case '\uFF5B':  // ï½›  [FULLWIDTH LEFT CURLY BRACKET]
							output[outputPos++] = '{';
							break;
						
						case '\u2775': 
						// â�µ  [MEDIUM RIGHT CURLY BRACKET ORNAMENT]
						case '\uFF5D':  // ï½�  [FULLWIDTH RIGHT CURLY BRACKET]
							output[outputPos++] = '}';
							break;
						
						case '\u207A': 
						// â�º  [SUPERSCRIPT PLUS SIGN]
						case '\u208A': 
						// â‚Š  [SUBSCRIPT PLUS SIGN]
						case '\uFF0B':  // ï¼‹  [FULLWIDTH PLUS SIGN]
							output[outputPos++] = '+';
							break;
						
						case '\u207C': 
						// â�¼  [SUPERSCRIPT EQUALS SIGN]
						case '\u208C': 
						// â‚Œ  [SUBSCRIPT EQUALS SIGN]
						case '\uFF1D':  // ï¼�  [FULLWIDTH EQUALS SIGN]
							output[outputPos++] = '=';
							break;
						
						case '\uFF01':  // ï¼�  [FULLWIDTH EXCLAMATION MARK]
							output[outputPos++] = '!';
							break;
						
						case '\u203C':  // â€¼  [DOUBLE EXCLAMATION MARK]
							output[outputPos++] = '!';
							output[outputPos++] = '!';
							break;
						
						case '\u2049':  // â�‰  [EXCLAMATION QUESTION MARK]
							output[outputPos++] = '!';
							output[outputPos++] = '?';
							break;
						
						case '\uFF03':  // ï¼ƒ  [FULLWIDTH NUMBER SIGN]
							output[outputPos++] = '#';
							break;
						
						case '\uFF04':  // ï¼„  [FULLWIDTH DOLLAR SIGN]
							output[outputPos++] = '$';
							break;
						
						case '\u2052': 
						// â�’  [COMMERCIAL MINUS SIGN]
						case '\uFF05':  // ï¼…  [FULLWIDTH PERCENT SIGN]
							output[outputPos++] = '%';
							break;
						
						case '\uFF06':  // ï¼†  [FULLWIDTH AMPERSAND]
							output[outputPos++] = '&';
							break;
						
						case '\u204E': 
						// â�Ž  [LOW ASTERISK]
						case '\uFF0A':  // ï¼Š  [FULLWIDTH ASTERISK]
							output[outputPos++] = '*';
							break;
						
						case '\uFF0C':  // ï¼Œ  [FULLWIDTH COMMA]
							output[outputPos++] = ',';
							break;
						
						case '\uFF0E':  // ï¼Ž  [FULLWIDTH FULL STOP]
							output[outputPos++] = '.';
							break;
						
						case '\u2044': 
						// â�„  [FRACTION SLASH]
						case '\uFF0F':  // ï¼�  [FULLWIDTH SOLIDUS]
							output[outputPos++] = '/';
							break;
						
						case '\uFF1A':  // ï¼š  [FULLWIDTH COLON]
							output[outputPos++] = ':';
							break;
						
						case '\u204F': 
						// â��  [REVERSED SEMICOLON]
						case '\uFF1B':  // ï¼›  [FULLWIDTH SEMICOLON]
							output[outputPos++] = ';';
							break;
						
						case '\uFF1F':  // ï¼Ÿ  [FULLWIDTH QUESTION MARK]
							output[outputPos++] = '?';
							break;
						
						case '\u2047':  // â�‡  [DOUBLE QUESTION MARK]
							output[outputPos++] = '?';
							output[outputPos++] = '?';
							break;
						
						case '\u2048':  // â�ˆ  [QUESTION EXCLAMATION MARK]
							output[outputPos++] = '?';
							output[outputPos++] = '!';
							break;
						
						case '\uFF20':  // ï¼   [FULLWIDTH COMMERCIAL AT]
							output[outputPos++] = '@';
							break;
						
						case '\uFF3C':  // ï¼¼  [FULLWIDTH REVERSE SOLIDUS]
							output[outputPos++] = '\\';
							break;
						
						case '\u2038': 
						// â€¸  [CARET]
						case '\uFF3E':  // ï¼¾  [FULLWIDTH CIRCUMFLEX ACCENT]
							output[outputPos++] = '^';
							break;
						
						case '\uFF3F':  // ï¼¿  [FULLWIDTH LOW LINE]
							output[outputPos++] = '_';
							break;
						
						case '\u2053': 
						// â�“  [SWUNG DASH]
						case '\uFF5E':  // ï½ž  [FULLWIDTH TILDE]
							output[outputPos++] = '~';
							break;
						
						default: 
							output[outputPos++] = c;
							break;
						
					}
				}
			}
		}
	}
}
