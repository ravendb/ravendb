// -----------------------------------------------------------------------
//  <copyright file="WildcardMatcher.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Util
{
	public class WildcardMatcher
	{
		public static bool Matches(string pattern, string input, int patternPos = 0, int inputPos = 0)
		{
			if (string.IsNullOrEmpty(pattern))
				return true;

			if (input == null)
				throw new ArgumentNullException("input");

			if(input.Length ==0)
				return false;

			for (int i = inputPos; i < input.Length; i++)
			{
				if (patternPos >= pattern.Length)
					return false; // input has more than the pattern
				var currentPatternChar = char.ToUpperInvariant(pattern[patternPos]);
				var currentInputChar = char.ToUpperInvariant(input[i]);

				switch (currentPatternChar)
				{
					case '*':
						// match a number of letters, need to check the _next_ pattern pos for a match, which will end 
						// our current * matching
						if(patternPos+1 < pattern.Length)
						{
							var nextPatternChar = char.ToUpperInvariant(pattern[patternPos + 1]);
							if (currentInputChar == nextPatternChar ||
								nextPatternChar == '?')
							{ // we have a match for the next part, let us see if it is an actual match

								if (Matches(pattern, input, patternPos + 1, i))
									return true;
							}
						}
						break;
					case '?': // matches any single letter
						patternPos++;
						break;
					default:
						if (currentInputChar != currentPatternChar)
							return false;
						patternPos++;
						break;
				}
			}

			return patternPos == pattern.Length ||
			       (patternPos == pattern.Length - 1 && pattern[patternPos] == '*');
		} 
	}
}