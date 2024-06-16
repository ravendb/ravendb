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

namespace Lucene.Net.Search.Highlight
{
	/// <summary>
	/// Formats text with different color intensity depending on the score of the term.
	/// </summary>
	public class GradientFormatter : IFormatter
	{
		private float maxScore;

        protected internal int fgRMin, fgGMin, fgBMin;
        protected internal int fgRMax, fgGMax, fgBMax;
		protected bool highlightForeground;
        protected internal int bgRMin, bgGMin, bgBMin;
        protected internal int bgRMax, bgGMax, bgBMax;
		protected bool highlightBackground;
		
		/// <summary> Sets the color range for the IDF scores</summary>
        /// <param name="maxScore">
		/// The score (and above) displayed as maxColor (See QueryScorer.getMaxWeight 
		/// which can be used to callibrate scoring scale)
		/// </param>
        /// <param name="minForegroundColor">
		/// The hex color used for representing IDF scores of zero eg
		/// #FFFFFF (white) or null if no foreground color required
		/// </param>
        /// <param name="maxForegroundColor">
		/// The largest hex color used for representing IDF scores eg
		/// #000000 (black) or null if no foreground color required
		/// </param>
        /// <param name="minBackgroundColor">
		/// The hex color used for representing IDF scores of zero eg
		/// #FFFFFF (white) or null if no background color required
		/// </param>
        /// <param name="maxBackgroundColor">
		/// The largest hex color used for representing IDF scores eg
		/// #000000 (black) or null if no background color required
		/// </param>
		public GradientFormatter(float maxScore, string minForegroundColor, string maxForegroundColor, string minBackgroundColor, string maxBackgroundColor)
		{
		    highlightForeground = (minForegroundColor != null) && (maxForegroundColor != null);

            if (highlightForeground)
			{
				if (minForegroundColor.Length != 7)
				{
					throw new ArgumentException("minForegroundColor is not 7 bytes long eg a hex " + "RGB value such as #FFFFFF");
				}
				if (maxForegroundColor.Length != 7)
				{
					throw new ArgumentException("minForegroundColor is not 7 bytes long eg a hex " + "RGB value such as #FFFFFF");
				}
				fgRMin = HexToInt(minForegroundColor.Substring(1, 2));
				fgGMin = HexToInt(minForegroundColor.Substring(3, 2));
				fgBMin = HexToInt(minForegroundColor.Substring(5, 2));
				
				fgRMax = HexToInt(maxForegroundColor.Substring(1, 2));
				fgGMax = HexToInt(maxForegroundColor.Substring(3, 2));
				fgBMax = HexToInt(maxForegroundColor.Substring(5, 2));
			}
			
			highlightBackground = (minBackgroundColor != null) && (maxBackgroundColor != null);
			if (highlightBackground)
			{
				if (minBackgroundColor.Length != 7)
				{
					throw new System.ArgumentException("minBackgroundColor is not 7 bytes long eg a hex " + "RGB value such as #FFFFFF");
				}
				if (maxBackgroundColor.Length != 7)
				{
					throw new System.ArgumentException("minBackgroundColor is not 7 bytes long eg a hex " + "RGB value such as #FFFFFF");
				}
				bgRMin = HexToInt(minBackgroundColor.Substring(1, 2));
				bgGMin = HexToInt(minBackgroundColor.Substring(3, 2));
				bgBMin = HexToInt(minBackgroundColor.Substring(5, 2));
				
				bgRMax = HexToInt(maxBackgroundColor.Substring(1, 2));
				bgGMax = HexToInt(maxBackgroundColor.Substring(3, 2));
				bgBMax = HexToInt(maxBackgroundColor.Substring(5, 2));
			}
			//        this.corpusReader = corpusReader;
			this.maxScore = maxScore;
			//        totalNumDocs = corpusReader.numDocs();
		}
		
		public virtual string HighlightTerm(string originalText, TokenGroup tokenGroup)
		{
			if (tokenGroup.TotalScore == 0)
				return originalText;
			float score = tokenGroup.TotalScore;
			if (score == 0)
			{
				return originalText;
			}

			var sb = new System.Text.StringBuilder();
			sb.Append("<font ");
			if (highlightForeground)
			{
				sb.Append("color=\"");
				sb.Append(GetForegroundColorString(score));
				sb.Append("\" ");
			}
			if (highlightBackground)
			{
				sb.Append("bgcolor=\"");
				sb.Append(GetBackgroundColorString(score));
				sb.Append("\" ");
			}
			sb.Append(">");
			sb.Append(originalText);
			sb.Append("</font>");
			return sb.ToString();
		}
		
		protected internal virtual string GetForegroundColorString(float score)
		{
			int rVal = GetColorVal(fgRMin, fgRMax, score);
			int gVal = GetColorVal(fgGMin, fgGMax, score);
			int bVal = GetColorVal(fgBMin, fgBMax, score);
			var sb = new System.Text.StringBuilder();
			sb.Append("#");
			sb.Append(IntToHex(rVal));
			sb.Append(IntToHex(gVal));
			sb.Append(IntToHex(bVal));
			return sb.ToString();
		}
		
		protected internal virtual string GetBackgroundColorString(float score)
		{
			int rVal = GetColorVal(bgRMin, bgRMax, score);
			int gVal = GetColorVal(bgGMin, bgGMax, score);
			int bVal = GetColorVal(bgBMin, bgBMax, score);
			var sb = new System.Text.StringBuilder();
			sb.Append("#");
			sb.Append(IntToHex(rVal));
			sb.Append(IntToHex(gVal));
			sb.Append(IntToHex(bVal));
			return sb.ToString();
		}
		
		private int GetColorVal(int colorMin, int colorMax, float score)
		{
			if (colorMin == colorMax)
			{
				return colorMin;
			}
			float scale = Math.Abs(colorMin - colorMax);
			float relScorePercent = Math.Min(maxScore, score) / maxScore;
			float colScore = scale * relScorePercent;
			return Math.Min(colorMin, colorMax) + (int) colScore;
		}
		
		private static char[] hexDigits = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
		
		private static string IntToHex(int i)
		{
			return "" + hexDigits[(i & 0xF0) >> 4] + hexDigits[i & 0x0F];
		}
		
		/// <summary> Converts a hex string into an int. Integer.parseInt(hex, 16) assumes the
		/// input is nonnegative unless there is a preceding minus sign. This method
		/// reads the input as twos complement instead, so if the input is 8 bytes
		/// long, it will correctly restore a negative int produced by
		/// Integer.toHexString() but not neccesarily one produced by
		/// Integer.toString(x,16) since that method will produce a string like '-FF'
		/// for negative integer values.
		/// 
		/// </summary>
        /// <param name="hex">
		/// A string in capital or lower case hex, of no more then 16
		/// characters.
		/// </param>
        /// <exception cref="FormatException">if the string is more than 16 characters long, or if any
		/// character is not in the set [0-9a-fA-f]</exception>
		public static int HexToInt(string hex)
		{
			int len = hex.Length;
			if (len > 16)
				throw new FormatException();
			
			int l = 0;
			for (int i = 0; i < len; i++)
			{
				l <<= 4;
				int c = (int) System.Char.GetNumericValue(hex[i]);
				if (c < 0)
					throw new FormatException();
				l |= c;
			}
			return l;
		}
	}
}