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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Lucene.Net.Util;

namespace Lucene.Net.Index
{
    /// <summary>
    /// The type of parser for the value of the term.
    /// </summary>
    public enum FieldParser
    {
        String,
        Numeric
    }

    /// <summary>
    /// <para>Base class for the typed enumerators.</para> 
    /// 
    /// <para>
    /// There are five implementations of FieldEnumerator<typeparamref name="T"/> for
    /// strings, integers, longs, floats, and doubles. The numeric enumerators support both 
    /// standard Field and NumericField implementations.  The string and numeric enumerators
    /// have slightly different options, but both should be used within a using statment
    /// to close the underlying TermEnum/TermDocs. Refer to the unit tests for usage examples.
    /// </para>
    /// </summary>
    /// <typeparam name="T">The type of data being enumerated.</typeparam>
    public abstract class FieldEnumerator<T> : IDisposable
    {
        /// <summary>
        /// Whether the enumerator will include TermDocs.
        /// </summary>
        protected bool includeDocs;

        /// <summary>
        /// The underlying TermEnum;
        /// </summary>
        private TermEnum termEnum;

        /// <summary>
        /// The optional TermDocs.
        /// </summary>
        private TermDocs termDocs;

        /// <summary>
        /// The specialized TermEnum enumerator.
        /// </summary>
        protected TermEnumerator tEnum;

        /// <summary>
        /// The specialized TermDoc enumerator.
        /// </summary>
        private TermDocEnumerator.TermDocUsingTermsEnumerator tdEnum;

        /// <summary>
        /// Whether or not the instance has been disposed.
        /// </summary>
        private bool disposed;

        /// <summary>
        /// Initialization method called by subclasses to simulate a shared
        /// base constructor as generic classes cannot have a parameterized ctor.
        /// </summary>
        /// <param name="reader">The index reader to read from.</param>
        /// <param name="field">The field to enumerate.</param>
        protected void Init(IndexReader reader, string field)
        {
            this.Init(reader, field, true);
        }

        /// <summary>
        /// Initialization method called by subclasses to simulate a shared
        /// base constructor as generic classes cannot have a parameterized ctor.
        /// </summary>
        /// <param name="reader">The index reader to read from.</param>
        /// <param name="fieldName">The field to enumerate.</param>
        /// <param name="includeDocs">Whether this enumerator will support TermDocs.</param>
        protected void Init(IndexReader reader, string fieldName, bool includeDocs)
        {
            this.termEnum = reader.Terms(new Term(fieldName));
            if (includeDocs)
            {
                this.termDocs = reader.TermDocs();
                this.tdEnum = new TermDocEnumerator.TermDocUsingTermsEnumerator(this.termDocs, this.termEnum);
            }
            this.tEnum = new TermEnumerator(termEnum, termDocs, fieldName, this);
        }

        /// <summary>
        /// Method to attempt to parse out the value from the encoded string
        /// and sets the value of Current.
        /// </summary>
        /// <param name="s">The encoded string.</param>
        /// <returns>True if the value was successfully parsed, false if we reached the
        /// end of encoded values in the fiele and only the tries remain.</returns>
        protected abstract bool TryParse(string s);

        /// <summary>
        /// Access the enumerator for the terms.
        /// </summary>
        public TermEnumerator Terms
        {
            get { return this.tEnum; }
        }

        /// <summary>
        /// Access the enumerator for the TermDocs.
        /// </summary>
        public TermDocEnumerator.TermDocUsingTermsEnumerator Docs
        {
            get
            {
                if (this.termDocs == null)
                {
                    throw new NotSupportedException("This instance does not support enumeration over the document ids.");
                }
                else
                {
                    return this.tdEnum;
                }
            }
        }

        #region IDisposable Members

        /// <summary>
        /// Dispose of the instance.
        /// </summary>
        public void Dispose()
        {
            if (!this.disposed)
            {
                this.disposed = true;
                if (this.termEnum != null)
                {
                    this.termEnum.Close();
                }
                if (this.termDocs != null)
                {
                    this.termDocs.Close();
                }
                GC.SuppressFinalize(this);
            }
        }

        #endregion

        /// <summary>
        /// The enumerator over the terms in an index.
        /// </summary>
        public class TermEnumerator : IEnumerator<T>, IEnumerable<T>
        {
            /// <summary>
            /// The underlying TermEnum;
            /// </summary>
            private TermEnum termEnum;

            /// <summary>
            /// The optional TermDocs.
            /// </summary>
            private TermDocs termDocs;

            /// <summary>
            /// The current term in the enum.
            /// </summary>
            private Term currentTerm;

            /// <summary>
            /// The field name, if any for the enum.
            /// </summary>
            protected string fieldName;

            /// <summary>
            /// Whether the enumerator has moved beyond the first position.
            /// </summary>
            private bool isFirst = true;

            /// <summary>
            /// THe enclosing instance, called back to in order to parse the field.
            /// </summary>
            private FieldEnumerator<T> enclosing;

            /// <summary>
            /// Ctor.
            /// </summary>
            /// <param name="termEnum">The TermEnum to wrap.</param>
            /// <param name="termDocs">The TermDocs to wrap.</param>
            /// <param name="field"> </param>
            /// <param name="enclosing"> </param>
            public TermEnumerator(TermEnum termEnum, TermDocs termDocs, string field, FieldEnumerator<T> enclosing)
            {
                this.termEnum = termEnum;
                this.termDocs = termDocs;
                this.fieldName = field;
                this.enclosing = enclosing;
            }

            #region IEnumerator<T> Members

            /// <summary>
            /// The current item in the enumerator.
            /// </summary>
            public T Current
            {
                get;
                internal set;
            }

            #endregion

            #region IEnumerator Members

            /// <summary>
            /// Current item in the enumerator.
            /// </summary>
            object IEnumerator.Current
            {
                get { return (object)this.Current; }
            }

            /// <summary>
            /// Advance to the next item.
            /// </summary>
            /// <returns></returns>
            public bool MoveNext()
            {
                if (this.isFirst)
                {
                    this.isFirst = false;
                }
                else
                {
                    if (!this.termEnum.Next())
                    {
                        return false;
                    }
                }

                this.currentTerm = termEnum.Term;
                if (this.currentTerm == null || (!this.currentTerm.Field.Equals(this.fieldName)))
                {
                    return false;
                }

                if (this.enclosing.TryParse(this.currentTerm.Text))
                {
                    if (this.termDocs != null)
                    {
                        this.termDocs.Seek(this.termEnum);
                    }
                    return true;
                }

                return false;
            }

            /// <summary>
            /// Reset the enumerator to the beginngin.
            /// </summary>
            public void Reset()
            {
                throw new NotSupportedException("The enumerator cannot be reset");
            }

            #endregion

            #region IDisposable Members

            public void Dispose()
            {
                // noop
            }

            #endregion

            #region IEnumerable<T> Members

            /// <summary>
            /// Accessor to IEnumerator-T-."/>
            /// </summary>
            /// <returns></returns>
            public IEnumerator<T> GetEnumerator()
            {
                return this;
            }

            #endregion

            #region IEnumerable Members

            /// <summary>
            /// Accessor to IEnumertor.
            /// </summary>
            /// <returns></returns>
            IEnumerator IEnumerable.GetEnumerator()
            {
                return this.GetEnumerator();
            }

            #endregion
        }
    }

    /// <summary>
    /// Class to handle creating a TermDocs and allowing for seeking and enumeration. Used
    /// when you have a set of one or moreterms for which you want to enumerate over the 
    /// documents that contain those terms.
    /// </summary>
    public class TermDocEnumerator : IEnumerable<int>, IDisposable
    {
        /// <summary>
        /// The underlying TermDocs.
        /// </summary>
        private TermDocs termDocs;

        /// <summary>
        /// Ctor.
        /// </summary>
        /// <param name="termDocs">The TermDocs to wrap.</param>
        public TermDocEnumerator(TermDocs termDocs)
        {
            this.termDocs = termDocs;
        }

        /// <summary>
        /// Seek to a specific term.
        /// </summary>
        /// <param name="t"></param>
        public void Seek(Term t)
        {
            this.termDocs.Seek(t);
        }

        #region IEnumerable<int> Members

        public IEnumerator<int> GetEnumerator()
        {
            return new TermDocUsingTermsEnumerator(this.termDocs);
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        #endregion

        #region IDisposable Members

        /// <summary>
        /// Dispose of the instance, closing the termdocs.
        /// </summary>
        public void Dispose()
        {
            if (this.termDocs != null)
            {
                termDocs.Close();
            }
        }

        #endregion

        /// <summary>
        /// Class to handle enumeration over the TermDocs that does NOT close them
        /// on a call to Dispose!
        /// </summary>
        public class TermDocUsingTermsEnumerator : IEnumerable<int>, IEnumerator<int>
        {
            /// <summary>
            /// A reference to an outside TermEnum that is used to position
            /// the TermDocs.  It can be null.
            /// </summary>
            private TermEnum termEnum;

            /// <summary>
            /// The underlying TermDocs.
            /// </summary>
            private TermDocs termDocs;

            /// <summary>
            /// Ctor.
            /// </summary>
            /// <param name="termDocs">TermDocs to wrap</param>
            internal TermDocUsingTermsEnumerator(TermDocs termDocs)
                : this(termDocs, null)
            { }

            /// <summary>
            /// Ctor.
            /// </summary>
            /// <param name="td">Underlying TermDocs.</param>
            /// <param name="termEnum">Enclosing field enum.</param>
            internal TermDocUsingTermsEnumerator(TermDocs td, TermEnum termEnum)
            {
                this.termDocs = td;
                this.termEnum = termEnum;
            }

            /// <summary>
            /// Seel to a term in the underlying TermDocs.
            /// </summary>
            /// <param name="te">The point to seek to.</param>
            internal void Seek(TermEnum te)
            {
                this.termDocs.Seek(te);
            }

            #region IEnumerable<int> Members

            /// <summary>
            /// Get the enumerator.
            /// </summary>
            /// <returns></returns>
            public IEnumerator<int> GetEnumerator()
            {
                return this;
            }

            #endregion

            #region IEnumerable Members

            /// <summary>
            /// Get the enumerator.
            /// </summary>
            /// <returns></returns>
            IEnumerator IEnumerable.GetEnumerator()
            {
                return this.GetEnumerator();
            }

            #endregion

            #region IEnumerator<int> Members

            /// <summary>
            /// The current document id.
            /// </summary>
            public int Current
            {
                get { return this.termDocs.Doc; }
            }

            #endregion

            #region IDisposable Members

            /// <summary>
            /// Dispose impl.
            /// </summary>
            public void Dispose()
            {
                // noop as the closing of the underlying
                // TermDocs is handled by the containing class
            }

            #endregion

            #region IEnumerator Members

            /// <summary>
            /// The current item.
            /// </summary>
            object IEnumerator.Current
            {
                get { throw new NotImplementedException(); }
            }

            /// <summary>
            /// Move to the next item.
            /// </summary>
            /// <returns>True if more, false if not.</returns>
            public bool MoveNext()
            {
                return this.termDocs.Next();
            }

            /// <summary>
            /// Not implemented. Use Seek instead.
            /// </summary>
            public void Reset()
            {
                throw new NotImplementedException();
            }

            #endregion
        }
    }


    /// <summary>
    /// Implementation for enumerating over terms with a string value.
    /// </summary>
    public class StringFieldEnumerator : FieldEnumerator<string>
    {
        /// <summary>
        /// Construct an enumerator over one field.
        /// </summary>
        /// <param name="reader">Index reader.</param>
        /// <param name="fieldName">The field to read.</param>
        public StringFieldEnumerator(IndexReader reader, string fieldName)
        {
            this.Init(reader, fieldName);
        }

        /// <summary>
        /// Construct an enumerator over one field.
        /// </summary>
        /// <param name="reader">Index reader.</param>
        /// <param name="fieldName">The field to read.</param>
        /// <param name="includeDocs">Whether the TermDocs will also be enumerated.</param>
        public StringFieldEnumerator(IndexReader reader, string fieldName, bool includeDocs)
        {
            this.Init(reader, fieldName, includeDocs);
        }

        /// <summary>
        /// Sets the value of current.
        /// </summary>
        /// <param name="s">The string.</param>
        /// <returns>Always true.</returns>
        protected override bool TryParse(string s)
        {
            this.tEnum.Current = s;
            return true;
        }
    }

    /// <summary>
    /// Base for enumerating over numeric fields.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class NumericFieldEnum<T> : FieldEnumerator<T>
    {
        /// <summary>
        /// The parser type for the field being enumerated.
        /// </summary>
        private FieldParser parser;

        /// <summary>
        /// Initialize the instance.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <param name="field">The field name.</param>
        /// <param name="includeDocs">Whether to include a TermDoc enum.</param>
        /// <param name="parser">The parser to use on the field.</param>
        protected void Init(IndexReader reader, string field, bool includeDocs, FieldParser parser)
        {
            base.Init(reader, field, includeDocs);
            this.parser = parser;
        }

        /// <summary>
        /// Overridden from base.
        /// </summary>
        /// <param name="s">String to parse.</param>
        /// <returns></returns>
        protected override bool TryParse(string s)
        {
            if (this.parser == FieldParser.Numeric)
            {
                return this.TryParseNumeric(s);
            }
            else
            {
                return this.TryParseString(s);
            }
        }

        /// <summary>
        /// Parse out a standard string and set the value of current.
        /// </summary>
        /// <param name="s">String to parse.</param>
        /// <returns>True on success.</returns>
        protected abstract bool TryParseString(string s);

        /// <summary>
        /// Parse out an encoded numeric string and set the value of current.
        /// </summary>
        /// <param name="s">String to parse.</param>
        /// <returns>True on success.</returns>
        protected abstract bool TryParseNumeric(string s);
    }

    /// <summary>
    /// Implementation for enumerating over all of the terms in an int numeric field.
    /// </summary>
    public class IntFieldEnumerator : NumericFieldEnum<int>
    {
        /// <summary>
        /// Construct an enumerator over one field.
        /// </summary>
        /// <param name="reader">Index reader.</param>
        /// <param name="fieldName">The field to read.</param>
        public IntFieldEnumerator(IndexReader reader, string fieldName, FieldParser parser)
        {
            this.Init(reader, fieldName, true, parser);
        }

        /// <summary>
        /// Construct an enumerator over one field.
        /// </summary>
        /// <param name="reader">Index reader.</param>
        /// <param name="fieldName">The field to read.</param>
        /// <param name="includeDocs">Whether the TermDocs will also be enumerated.</param>
        public IntFieldEnumerator(IndexReader reader, string fieldName, FieldParser parser, bool includeDocs)
        {
            this.Init(reader, fieldName, includeDocs, parser);
        }

        /// <summary>
        /// Parse the int from the string.
        /// </summary>
        /// <param name="s">String to parse.</param>
        /// <returns>Always true.</returns>
        protected override bool TryParseString(string s)
        {
            this.tEnum.Current = Int32.Parse(s);
            return true;
        }

        /// <summary>
        /// Parse the int from an encoded string.
        /// </summary>
        /// <param name="s">String to parse.</param>
        /// <returns>True if the parse was successful, false indicating failure
        /// and the end of useful terms in the numeric field.</returns>
        protected override bool TryParseNumeric(string s)
        {
            int shift = s[0] - NumericUtils.SHIFT_START_INT;
            if (shift > 0 && shift <= 31)
            {
                return false;
            }
            else
            {
                this.tEnum.Current = NumericUtils.PrefixCodedToInt(s);
                return true;
            }
        }
    }

    /// <summary>
    /// Implementation for enumerating over all of the terms in a float numeric field.
    /// </summary>
    public class FloatFieldEnumerator : NumericFieldEnum<float>
    {

        /// <summary>
        /// Construct an enumerator over one field.
        /// </summary>
        /// <param name="reader">Index reader.</param>
        /// <param name="fieldName">The field to read.</param>
        public FloatFieldEnumerator(IndexReader reader, string fieldName, FieldParser parser)
        {
            this.Init(reader, fieldName, true, parser);
        }

        /// <summary>
        /// Construct an enumerator over one field.
        /// </summary>
        /// <param name="reader">Index reader.</param>
        /// <param name="fieldName">The field to read.</param>
        /// <param name="includeDocs">Whether the TermDocs will also be enumerated.</param>
        public FloatFieldEnumerator(IndexReader reader, string fieldName, FieldParser parser, bool includeDocs)
        {
            this.Init(reader, fieldName, includeDocs, parser);
        }

        /// <summary>
        /// Parse the float from a string.
        /// </summary>
        /// <param name="s">The string to parse.</param>
        /// <returns>Always true.</returns>
        protected override bool TryParseString(string s)
        {
            this.tEnum.Current = float.Parse(s);
            return true;
        }

        /// <summary>
        /// Parse the float from a numeric field.
        /// </summary>
        /// <param name="s">The string to parse.</param>
        /// <returns>True if the string was parsed, false to signify the
        /// end of usable terms in a numeric field.</returns>
        protected override bool TryParseNumeric(string s)
        {
            int shift = s[0] - NumericUtils.SHIFT_START_INT;
            if (shift > 0 && shift <= 31)
            {
                return false;
            }
            else
            {
                this.tEnum.Current = NumericUtils.SortableIntToFloat(NumericUtils.PrefixCodedToInt(s));
                return true;
            }
        }
    }

    /// <summary>
    /// Implementation for enumerating over all of the terms in a double numeric field.
    /// </summary>
    public class DoubleFieldEnumerator : NumericFieldEnum<double>
    {
        /// <summary>
        /// Construct an enumerator over one field.
        /// </summary>
        /// <param name="reader">Index reader.</param>
        /// <param name="fieldName">The field to read.</param>
        public DoubleFieldEnumerator(IndexReader reader, string fieldName, FieldParser parser)
        {
            this.Init(reader, fieldName, true, parser);
        }

        /// <summary>
        /// Construct an enumerator over one field.
        /// </summary>
        /// <param name="reader">Index reader.</param>
        /// <param name="fieldName">The field to read.</param>
        /// <param name="includeDocs">Whether the TermDocs will also be enumerated.</param>
        public DoubleFieldEnumerator(IndexReader reader, string fieldName, FieldParser parser, bool includeDocs)
        {
            this.Init(reader, fieldName, includeDocs, parser);
        }

        /// <summary>
        /// Parse the double from a string.
        /// </summary>
        /// <param name="s">The string to parse.</param>
        /// <returns>Always true.</returns>
        protected override bool TryParseString(string s)
        {
            this.tEnum.Current = Double.Parse(s);
            return true;
        }

        /// <summary>
        /// Parse the double from a numeric field.
        /// </summary>
        /// <param name="s">The string to parse.</param>
        /// <returns>True if the string was parsed, false to indicate the end
        /// of usable numeric terms.</returns>
        protected override bool TryParseNumeric(string s)
        {
            int shift = s[0] - NumericUtils.SHIFT_START_LONG;
            if (shift > 0 && shift <= 63)
            {
                return false;
            }
            else
            {
                this.tEnum.Current = NumericUtils.SortableLongToDouble(NumericUtils.PrefixCodedToLong(s));
                return true;
            }
        }
    }

    /// <summary>
    /// Implementation for enumerating over all of the terms in a long numeric field.
    /// </summary>
    public class LongFieldEnumerator : NumericFieldEnum<long>
    {
        /// <summary>
        /// Construct an enumerator over one field.
        /// </summary>
        /// <param name="reader">Index reader.</param>
        /// <param name="fieldName">The field to read.</param>
        public LongFieldEnumerator(IndexReader reader, string fieldName, FieldParser parser)
        {
            this.Init(reader, fieldName, true, parser);
        }

        /// <summary>
        /// Construct an enumerator over one field.
        /// </summary>
        /// <param name="reader">Index reader.</param>
        /// <param name="fieldName">The field to read.</param>
        /// <param name="includeDocs">Whether the TermDocs will also be enumerated.</param>
        public LongFieldEnumerator(IndexReader reader, string fieldName, FieldParser parser, bool includeDocs)
        {
            this.Init(reader, fieldName, includeDocs, parser);
        }

        /// <summary>
        /// Parse the long from a string.
        /// </summary>
        /// <param name="s">The string to parse.</param>
        /// <returns>Always true.</returns>
        protected override bool TryParseString(string s)
        {
            this.tEnum.Current = long.Parse(s);
            return true;
        }

        /// <summary>
        /// Parse the long from a numeric field.
        /// </summary>
        /// <param name="s">The string to parse.</param>
        /// <returns>True if the string was parsed, false to inidicate
        /// the end of usable terms in a numeric field.</returns>
        protected override bool TryParseNumeric(string s)
        {
            int shift = s[0] - NumericUtils.SHIFT_START_LONG;
            if (shift > 0 && shift <= 63)
            {
                return false;
            }
            else
            {
                this.tEnum.Current = NumericUtils.PrefixCodedToLong(s);
                return true;
            }
        }
    }
}
