using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Text;


namespace Raven.NewClient.Abstractions.Util
{
    public class TextFieldParser : IDisposable
    {
        // TextFieldParser translation from VB (AA::commentTokens not implemented)
        private TextReader reader;
        private bool leaveOpen = false;
        // private string[] commentTokens = new string[];
        private string[] delimiters = null;
        private string errorLine = string.Empty;
        private long errorLineNumber = -1;
        private int[] fieldWidths = null;
        private bool hasFieldsEnclosedInQuotes = true;
        private long lineNumber = -1;
        public enum FieldType { Delimited, FixedWidth };
        private FieldType textFieldType = FieldType.Delimited;

        private bool trimWhiteSpace = true;
        private Queue<string> peekedLine = new Queue<string>();

        private int minFieldLength;

        public TextFieldParser(Stream stream)
        {
            reader = new StreamReader(stream);
        }

        public TextFieldParser(TextReader reader)
        {
            this.reader = reader;
        }

        public TextFieldParser(Stream stream, Encoding defaultEncoding)
        {
            reader = new StreamReader(stream, defaultEncoding);
        }

        public TextFieldParser(Stream stream, Encoding defaultEncoding, bool detectEncoding)
        {
            reader = new StreamReader(stream, defaultEncoding, detectEncoding);
        }


        public TextFieldParser(Stream stream, Encoding defaultEncoding, bool detectEncoding, bool leaveOpen)
        {
            reader = new StreamReader(stream, defaultEncoding, detectEncoding);
            this.leaveOpen = leaveOpen;
        }

        private string[] GetDelimitedFields()
        {
            if (delimiters == null || delimiters.Length == 0)
            {
                throw new InvalidOperationException("Unable to read delimited fields because Delimiters is Nothing or empty.");
            }

            List<string> result = new List<string>();
            string line = null;
            int currentIndex = 0;
            int nextIndex = 0;

            line = GetNextLine();

            if (line == null)
                return null;

            while (!(nextIndex >= line.Length))
            {
                result.Add(GetNextField(line, currentIndex, ref nextIndex));
                currentIndex = nextIndex;
            }

            return result.ToArray();
        }

        private string GetNextField(string line, int startIndex, ref int nextIndex)
        {
            bool inQuote = false;

            if (nextIndex == int.MinValue)
            {
                nextIndex = int.MaxValue;
                return string.Empty;
            }

            if (hasFieldsEnclosedInQuotes && line[startIndex] == '"')
            {
                inQuote = true;
                startIndex += 1;
            }

            bool mustMatch = false;
            for (int j = startIndex; j <= line.Length - 1; j++)
            {
                if (inQuote)
                {
                    if (line[j] == '"')
                    {
                        inQuote = false;
                        mustMatch = true;
                    }
                    continue;
                }

                for (int i = 0; i <= delimiters.Length - 1; i++)
                {
                    if (string.Compare(line, j, delimiters[i], 0, delimiters[i].Length) == 0)
                    {
                        nextIndex = j + delimiters[i].Length;
                        if (nextIndex == line.Length)
                        {
                            nextIndex = int.MinValue;
                        }
                        if (mustMatch)
                        {
                            return line.Substring(startIndex, j - startIndex - 1);
                        }
                        else {
                            return line.Substring(startIndex, j - startIndex);
                        }
                    }
                }

                if (mustMatch)
                {
                    RaiseDelimiterEx(line);
                }
            }

            if (inQuote)
            {
                RaiseDelimiterEx(line);
            }

            nextIndex = line.Length;
            if (mustMatch)
            {
                return line.Substring(startIndex, nextIndex - startIndex - 1);
            }
            else {
                return line.Substring(startIndex);
            }
        }

        private void RaiseDelimiterEx(string Line)
        {
            errorLineNumber = lineNumber;
            errorLine = Line;
            throw new MalformedLineException("Line " + errorLineNumber.ToString() + " cannot be parsed using the current Delimiters");
        }

        private void RaiseFieldWidthEx(string Line)
        {
            errorLineNumber = lineNumber;
            errorLine = Line;
            throw new MalformedLineException("Line " + errorLineNumber.ToString() + " cannot be parsed using the current FieldWidths.");
        }

        private string[] GetWidthFields()
        {
            if (fieldWidths == null || fieldWidths.Length == 0)
            {
                throw new InvalidOperationException("Unable to read fixed width fields because FieldWidths is Nothing or empty.");
            }

            string[] result = new string[fieldWidths.Length];
            int currentIndex = 0;
            string line = null;

            line = GetNextLine();

            if (line.Length < minFieldLength)
            {
                RaiseFieldWidthEx(line);
            }

            for (int i = 0; i <= result.Length - 1; i++)
            {
                if (trimWhiteSpace)
                {
                    result[i] = line.Substring(currentIndex, fieldWidths[i]).Trim();
                }
                else {
                    result[i] = line.Substring(currentIndex, fieldWidths[i]);
                }
                currentIndex += fieldWidths[i];
            }

            return result;
        }

        private bool IsCommentLine(string Line)
        {
            /*
            if (commentTokens == null)
                return false;

            foreach (string str in commentTokens) {
                if (Line.StartsWith(str))
                    return true;
            }
*/
            return false;
        }

        private string GetNextRealLine()
        {
            string nextLine = null;

            do
            {
                nextLine = ReadLine();
            } while (!(nextLine == null || IsCommentLine(nextLine) == false));

            return nextLine;
        }

        private string GetNextLine()
        {
            if (peekedLine.Count > 0)
            {
                return peekedLine.Dequeue();
            }
            else {
                return GetNextRealLine();
            }
        }



        public void Close()
        {
            if (reader != null && leaveOpen == false)
            {
                reader.Dispose();
            }
            reader = null;
        }

        ~TextFieldParser()
        {
            Dispose(false);
            // base.Finalize();
        }

        public string PeekChars(int numberOfChars)
        {
            if (numberOfChars < 1)
                throw new ArgumentException("numberOfChars has to be a positive, non-zero number", "numberOfChars");

            string[] peekedLines = null;
            string theLine = null;
            if (peekedLine.Count > 0)
            {
                peekedLines = peekedLine.ToArray();
                for (int i = 0; i <= peekedLine.Count - 1; i++)
                {
                    if (IsCommentLine(peekedLines[i]) == false)
                    {
                        theLine = peekedLines[i];
                        break; // TODO: might not be correct. Was : Exit For
                    }
                }
            }

            if (theLine == null)
            {
                do
                {
                    theLine = reader.ReadLine();
                    peekedLine.Enqueue(theLine);
                } while (!(theLine == null || IsCommentLine(theLine) == false));
            }

            if (theLine != null)
            {
                if (theLine.Length <= numberOfChars)
                {
                    return theLine;
                }
                else {
                    return theLine.Substring(0, numberOfChars);
                }
            }
            else {
                return null;
            }
        }

        public string[] ReadFields()
        {
            switch (textFieldType)
            {
                case FieldType.Delimited:
                    return GetDelimitedFields();
                case FieldType.FixedWidth:
                    return GetWidthFields();
                default:
                    return GetDelimitedFields();
            }
        }

        [EditorBrowsable(EditorBrowsableState.Advanced)]
        public string ReadLine()
        {
            if (peekedLine.Count > 0)
            {
                return peekedLine.Dequeue();
            }
            if (lineNumber == -1)
            {
                lineNumber = 1;
            }
            else {
                lineNumber += 1;
            }
            return reader.ReadLine();
        }

        [EditorBrowsable(EditorBrowsableState.Advanced)]
        public string ReadToEnd()
        {
            return reader.ReadToEnd();
        }

        public void SetDelimiters(params string[] delimiters)
        {
            this.Delimiters = delimiters;
            //m_TextFieldType = FieldType.Delimited
        }

        public void SetFieldWidths(params int[] fieldWidths)
        {
            this.FieldWidths = fieldWidths;
            //m_TextFieldType = FieldType.FixedWidth
        }
        /*
                [EditorBrowsable(EditorBrowsableState.Advanced)]
                public string[] CommentTokens {
                    get { return commentTokens; }
                    set { commentTokens = value; }
                }
        */
        public string[] Delimiters
        {
            get { return delimiters; }
            set { delimiters = value; }
        }

        public bool EndOfData
        {
            get { return PeekChars(1) == null; }
        }

        public string ErrorLine
        {
            get { return errorLine; }
        }

        public long ErrorLineNumber
        {
            get { return errorLineNumber; }
        }

        public int[] FieldWidths
        {
            get { return fieldWidths; }
            set
            {
                fieldWidths = value;
                if (fieldWidths != null)
                {
                    minFieldLength = 0;
                    for (int i = 0; i <= fieldWidths.Length - 1; i++)
                    {
                        minFieldLength += value[i];
                    }
                }
            }
        }

        [EditorBrowsable(EditorBrowsableState.Advanced)]
        public bool HasFieldsEnclosedInQuotes
        {
            get { return hasFieldsEnclosedInQuotes; }
            set { hasFieldsEnclosedInQuotes = value; }
        }

        [EditorBrowsable(EditorBrowsableState.Advanced)]
        public long LineNumber
        {
            get { return lineNumber; }
        }

        public FieldType TextFieldType
        {
            get { return textFieldType; }
            set { textFieldType = value; }
        }

        public bool TrimWhiteSpace
        {
            get { return trimWhiteSpace; }
            set { trimWhiteSpace = value; }
        }

        // To detect redundant calls
        private bool disposedValue = false;

        // IDisposable
        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposedValue)
            {
                Close();
            }
            this.disposedValue = true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

    }
}

