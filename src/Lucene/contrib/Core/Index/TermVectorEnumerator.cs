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

namespace Lucene.Net.Index
{
    /// <summary>
    /// Class to allow for enumerating over the documents in the index to 
    /// retrieve the term vector for each one.
    /// </summary>
    public class TermVectorEnumerator : IEnumerator<ITermFreqVector>, IEnumerable<ITermFreqVector>
    {
        /// <summary>
        /// Current document being accessed.
        /// </summary>
        private int document = -1;

        /// <summary>
        /// The field name the vectors are being accessed from.
        /// </summary>
        private string fieldName;

        /// <summary>
        /// The index reader that the vetors are retreived from.
        /// </summary>
        private IndexReader reader;

        /// <summary>
        /// The return value should a document be deleted.
        /// </summary>
        private EmptyVector emptyVector;

        /// <summary>
        /// Ctor.
        /// </summary>
        /// <param name="reader">The index reader used to read the vectors.</param>
        /// <param name="field">The name of the field the vectors are read from.</param>
        public TermVectorEnumerator(IndexReader reader, string field)
        {
            this.reader = reader;
            this.fieldName = field;
            this.emptyVector = new EmptyVector(field);
        }

        #region IEnumerator<TermFreqVector> Members

        public ITermFreqVector Current
        {
            get { return this.CurrentVector(); }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            // this is a noop as we do not want to close the reader
        }

        #endregion

        #region IEnumerator Members

        object IEnumerator.Current
        {
            get { return this.CurrentVector(); }
        }

        public bool MoveNext()
        {
            this.document++;
            return this.document < this.reader.MaxDoc;
        }

        public void Reset()
        {
            this.document = 0;
        }

        #endregion

        #region IEnumerable<TermFreqVector> Members

        public IEnumerator<ITermFreqVector> GetEnumerator()
        {
            return (IEnumerator<ITermFreqVector>)this;
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return (IEnumerator<ITermFreqVector>)this;
        }

        #endregion

        /// <summary>
        /// Retrieve the current TermFreqVector from the index.
        /// </summary>
        /// <returns>The current TermFreqVector.</returns>
        private ITermFreqVector CurrentVector()
        {
            if (this.reader.IsDeleted(this.document))
            {
                return this.emptyVector;
            }
            else
            {
                ITermFreqVector vector = this.reader.GetTermFreqVector(this.document, this.fieldName);
                if (vector == null)
                {
                    vector = this.emptyVector;
                }
                return vector;
            }
        }
    }

    /// <summary>
    /// A simple TermFreqVector implementation for an empty vector for use
    /// with a deleted document or a document that does not have the field
    /// that is being enumerated.
    /// </summary>
    public class EmptyVector : ITermFreqVector
    {
        private string field;

        private string[] emptyString = new string[0];

        private int[] emptyInt = new int[0];

        public EmptyVector(string field)
        {
            this.field = field;
        }

        #region TermFreqVector Members

        public string Field
        {
            get { return this.field; }
        }

        public int Size
        {
            get { return 0; }
        }

        public string[] GetTerms()
        {
            return this.emptyString;
        }

        public int[] GetTermFrequencies()
        {
            return this.emptyInt;
        }

        public int IndexOf(string term)
        {
            return 0;
        }

        public int[] IndexesOf(string[] terms, int start, int len)
        {
            return this.emptyInt;
        }

        #endregion
    }
}
