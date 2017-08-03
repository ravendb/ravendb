//-----------------------------------------------------------------------
// <copyright file="RavenQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace Raven.Abstractions.Util
{
    /// <summary>
    /// Helper class that provide a way to escape query terms
    /// </summary>
    public static class RavenQuery
    {
        /// <summary>
        /// Escapes Lucene operators and quotes phrases
        /// </summary>
        public static string Escape(string term)
        {
            return Escape(term, false, true);
        }

        /// <summary>
        /// Escapes Lucene operators and quotes phrases
        /// </summary>
        /// <returns>escaped term</returns>
        /// <remarks>
        /// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Escaping%20Special%20Characters
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string Escape(string term, bool allowWildcards, bool makePhrase)
        {
            //Needed to add a wrapper because you can't use defult parameter from lambda expression and adding it will break code
            return EscapeInternal(term, allowWildcards, makePhrase, false);
        }


        public static bool IsEscapedChar(char c)
        {
            return escapeChars.Contains(c);
        }

        private static HashSet<char> escapeChars = new HashSet<char> {'*', '?', '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '~', '/', ':','\"','\\'};

        public static string EscapeInternal(string term, bool allowWildcards, bool makePhrase, bool nested)
        {
            // method doesn't allocate a StringBuilder unless the string requires escaping
            // also this copies chunks of the original string into the StringBuilder which
            // is far more efficient than copying character by character because StringBuilder
            // can access the underlying string data directly

            if (string.IsNullOrEmpty(term))
            {
                return "\"\"";
            }
            int start = 0;
            int length = term.Length;
            StringBuilder buffer = null;
            for (int i = start; i < length; i++)
            {
                char ch = term[i];
                switch (ch)
                {
                    // should wildcards be included or excluded here?
                    case '*':
                    case '?':
                        {
                            if (allowWildcards)
                            {
                                break;
                            }
                            goto case ':';
                        }
                    case '+':
                    case '-':
                    case '&':
                    case '|':
                    case '!':
                    case '(':
                    case ')':
                    case '{':
                    case '}':
                    case '[':
                    case ']':
                    case '^':
                    case '~':
                    case '/':
                    case ':':
                        {
                            if (buffer == null)
                            {
                                // allocate builder with headroom
                                buffer = new StringBuilder(length * 2);
                            }

                            if (i > start)
                            {
                                // append any leading substring
                                buffer.Append(term, start, i - start);
                            }
                            if (nested == false) 
                            {
                                buffer.Append('\\').Append(ch);
                                start = i + 1;
                            }
                            else //We don't want to escape lucene special chars when already in phrase mode
                            {
                                buffer.Append(ch);
                                start = i + 1;
                            }
                            break;
                        }
                    case '"':
                    case '\\':
                        {
                            if (buffer == null)
                            {
                                // allocate builder with headroom
                                buffer = new StringBuilder(length * 2);
                            }

                            if (i > start)
                            {
                                // append any leading substring
                                buffer.Append(term, start, i - start);
                            }

                            buffer.Append('\\').Append(ch);
                            start = i + 1;
                            break;
                        }
                    case ' ':
                    case '\t':
                        {
                            if (makePhrase)
                            {
                                return new StringBuilder(EscapeInternal(term, allowWildcards, false, nested:true)).Insert(0,"\"").Append("\"").ToString();								
                            }
                            break;
                        }
                }
            }

            if (buffer == null)
            {
                switch (term)
                {
                    case "OR":
                        return "\"OR\"";
                    case "AND":
                        return "\"AND\"";
                    case "NOT":
                        return "\"NOT\"";
                    case "TO":
                        return "\"TO\"";
                    case "INTERSECT":
                        return "\"INTERSECT\"";
                    case "NULL":
                        return "\"NULL\"";
                    default:
                        return term;
                }
            }

            if (length > start)
            {
                // append any trailing substring
                buffer.Append(term, start, length - start);
            }

            return buffer.ToString();
        }

        private static readonly HashSet<char> fieldChars = new HashSet<char>
        {
            '*',
            '?',
            '+',
            '&',
            '|',
            '!',
            '(',
            ')',
            '{',
            '}',
            '[',
            ']',
            '^',
            '"',
            '~',
            '\\',
            ':',
            ' ',
            '\t'
        };
        /// <summary>
        /// Escapes Lucene field
        /// </summary>
        public static string EscapeField(string field)
        {
            // method doesn't allocate a StringBuilder unless the string requires escaping
            // also this copies chunks of the original string into the StringBuilder which
            // is far more efficient than copying character by character because StringBuilder
            // can access the underlying string data directly

            if (string.IsNullOrEmpty(field))
            {
                return "\"\"";
            }

            int start = 0;
            int length = field.Length;
            StringBuilder buffer = null;

            for (int i = start; i < length; i++)
            {
                char ch = field[i];

                if (ch == '\\')
                {
                    if (i + 1 < length && fieldChars.Contains(field[i + 1]))
                    {
                        i++; // skip next, since it was escaped
                        continue;
                    }
                }
                else if (!fieldChars.Contains(ch))
                    continue;

                if (buffer == null)
                {
                    // allocate builder with headroom
                    buffer = new StringBuilder(length * 2);
                }

                if (i > start)
                {
                    // append any leading substring
                    buffer.Append(field, start, i - start);
                }

                buffer.Append('\\').Append(ch);
                start = i + 1;
            }

            if (buffer == null)
                return field;

            if (length > start)
            {
                // append any trailing substring
                buffer.Append(field, start, length - start);
            }



            return buffer.ToString();
        }
    }


}
