// -----------------------------------------------------------------------
//  <copyright file="WildcardMatcher.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

namespace Raven.Abstractions.Util
{
    public class WildcardMatcher
    {
        private static readonly char[] Separator = new[] { '|' };

        public static bool Matches(string pattern, string input)
        {
            if (string.IsNullOrEmpty(pattern))
                return true;

            return MatchesImpl(pattern, input);
        }

        public static bool MatchesExclusion(string pattern, string input)
        {
            // null or empty means no match
            if (string.IsNullOrEmpty(pattern))
                return false;

            return MatchesImpl(pattern, input);
        }

        private static bool MatchesImpl(string pattern, string input)
        {
            var patterns = pattern.Split(Separator, StringSplitOptions.RemoveEmptyEntries);
            if (pattern.Length == 0)
                return false;

            return patterns.Any(p => MatchesImpl(p, input, 0, 0));
        }

        private static bool MatchesImpl(string pattern, string input, int patternPos, int inputPos)
        {
            if (string.IsNullOrEmpty(pattern))
                return true;

            if (input == null)
                throw new ArgumentNullException("input");

            if (input.Length == 0)
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
                        if (patternPos + 1 < pattern.Length)
                        {
                            var nextPatternChar = char.ToUpperInvariant(pattern[patternPos + 1]);
                            if (currentInputChar == nextPatternChar ||
                                nextPatternChar == '?')
                            { // we have a match for the next part, let us see if it is an actual match

                                if (MatchesImpl(pattern, input, patternPos + 1, i))
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
