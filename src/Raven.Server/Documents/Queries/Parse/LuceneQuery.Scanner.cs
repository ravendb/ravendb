using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Queries.Parse
{
    internal partial class LuceneQueryScanner
    {
        public bool InMethod;

        private static int FindUnescapedCommaIndex(string str, bool lookFromTheEnd = false)
        {
            int commaIndex;
            int cutoffIndex = lookFromTheEnd ? str.Length - 1 : 0;

            do
            {
                commaIndex = lookFromTheEnd ? str.LastIndexOf(',', cutoffIndex) : str.IndexOf(',', cutoffIndex);
                if(commaIndex == -1 || //didn't find any commas
                   commaIndex == 0 || //first character in a string is a comma - this means we found unescaped string -> ",`" is not escaped comma
                   commaIndex == str.Length - 1) //last character in a string is a comma - this means we found unescaped string -> "`," is not escaped comma
                    break;
                
                cutoffIndex = lookFromTheEnd ? commaIndex - 1 : commaIndex + 1;
            } while (!(str[commaIndex] == ',' &&
                      (commaIndex == 0 || str[commaIndex - 1] != '`') &&
                      (commaIndex == (str.Length - 1) || str[commaIndex + 1] != '`')));

            return commaIndex;
        }

        public string HandleTermInMethod()
        {
            var commaIndex = FindUnescapedCommaIndex(yytext);
            if (commaIndex == -1) 
                return DiscardEscapeChar(yytext,true);

            var firstTerm = DiscardEscapeChar(yytext.Substring(0, commaIndex), true);
            var lastCommaIndex = FindUnescapedCommaIndex(yytext, true);

            var newSource = yytext.Substring(commaIndex, lastCommaIndex - commaIndex);
            var rewind = yytext.Length - lastCommaIndex - 1;
            tokTxt = null;
            tokECol -= rewind;
            tokEPos -= rewind;
            buffer.Pos -= (rewind + 1);
            code = 44;
            yylloc = new LexLocation(tokLin - 1, tokCol, tokELin - 1, tokECol);
            if (lastCommaIndex != commaIndex)
            {
                var currentContext = MkBuffCtx();
                SetSource(newSource,0);

                var buildBuffer = buffer as StringBuffer;
                if (buildBuffer != null)  //precaution
                    buildBuffer.SetPaddingOn = true;

                bStack.Push(currentContext);
            }
            //CommaPaddingStream
            return firstTerm;
        }

        public void PublicSetSource(string source)
        {
            SetSource(source, 0);
        }

        protected override bool yywrap()
        {
            if (bStack.Count == 0) return true;
            RestoreBuffCtx(bStack.Pop()); return false;
        }
        Stack<BufferContext> bStack = new Stack<BufferContext>();
        public override void yyerror(string format, params object[] args)
        {
            base.yyerror(format, args);
            throw new Lucene.Net.QueryParsers.ParseException(string.Format(format, args));
        }
        /// <summary> Returns a String where the escape char has been
        /// removed, or kept only once if there was a double escape.
        /// 
        /// Supports escaped unicode characters, e. g. translates
        /// <c>\\u0041</c> to <c>A</c>.
        /// 
        /// </summary>
        private String DiscardEscapeChar(String input, bool shouldEscapeCommas = false)
        {
            if (input.IndexOf('\\') == -1)
            {
                return shouldEscapeCommas ? input.Replace("`,`", ",") : input;
            }
            // Create char array to hold unescaped char sequence
            var output = new char[input.Length];

            // The Length of the output can be less than the input
            // due to discarded escape chars. This variable holds
            // the actual Length of the output
            int Length = 0;

            // We remember whether the last processed character was 
            // an escape character
            bool lastCharWasEscapeChar = false;

            // The multiplier the current unicode digit must be multiplied with.
            // E. g. the first digit must be multiplied with 16^3, the second with 16^2...
            int codePointMultiplier = 0;

            // Used to calculate the codepoint of the escaped unicode character
            int codePoint = 0;

            var isSkippingEscapedComma = false;
            for (int i = 0; i < input.Length; i++)
            {
                var curChar = input[i];

                if (shouldEscapeCommas)
                {
                    if (curChar == '`' &&
                        (i + 2) < input.Length &&
                        input[i + 1] == ',' &&
                        input[i + 2] == '`')
                    {
                        isSkippingEscapedComma = true;
                        continue;
                    }

                    if (isSkippingEscapedComma)
                    {
                        if (curChar == '`' && i > 0 && input[i - 1] == ',')
                        {
                            output[Length++] = ',';
                            isSkippingEscapedComma = false;
                        }
                        continue;
                    }
                }

                if (codePointMultiplier > 0)
                {
                    codePoint += HexToInt(curChar) * codePointMultiplier;
                    codePointMultiplier = URShift(codePointMultiplier, 4);
                    if (codePointMultiplier == 0)
                    {
                        output[Length++] = (char)codePoint;
                        codePoint = 0;
                    }
                }
                else if (lastCharWasEscapeChar)
                {
                    if (curChar == 'u')
                    {
                        // found an escaped unicode character
                        codePointMultiplier = 16 * 16 * 16;
                    }
                    else
                    {
                        // this character was escaped
                        output[Length] = curChar;
                        Length++;
                    }
                    lastCharWasEscapeChar = false;
                }
                else
                {
                    if (curChar == '\\')
                    {
                        lastCharWasEscapeChar = true;
                    }
                    else
                    {
                        output[Length] = curChar;
                        Length++;
                    }
                }
            }

            if (codePointMultiplier > 0)
            {
                throw new Exception("Truncated unicode escape sequence.");
            }

            if (lastCharWasEscapeChar)
            {
                throw new Exception("Term can not end with escape character.");
            }

            return new String(output, 0, Length);
        }

        /// <summary>Returns the numeric value of the hexadecimal character </summary>
        private static int HexToInt(char c)
        {
            if ('0' <= c && c <= '9')
            {
                return c - '0';
            }
            else if ('a' <= c && c <= 'f')
            {
                return c - 'a' + 10;
            }
            else if ('A' <= c && c <= 'F')
            {
                return c - 'A' + 10;
            }
            else
            {
                throw new Exception("None-hex character in unicode escape sequence: " + c);
            }
        }
        /// <summary>
        /// Performs an unsigned bitwise right shift with the specified number
        /// </summary>
        /// <param name="number">Number to operate on</param>
        /// <param name="bits">Ammount of bits to shift</param>
        /// <returns>The resulting number from the shift operation</returns>
        public static int URShift(int number, int bits)
        {
            return (int)(((uint)number) >> bits);
        }

    }
}
