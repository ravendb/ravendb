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
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Lucene.Net.Search;
using Lucene.Net.Index;
using Lucene.Net.Util;

namespace Lucene.Net.Search
{
    public class DuplicateFilter : Filter
    {
        String fieldName;

        /*
         * KeepMode determines which document id to consider as the master, all others being 
         * identified as duplicates. Selecting the "first occurrence" can potentially save on IO.
         */
        int keepMode = KM_USE_FIRST_OCCURRENCE;
        public static int KM_USE_FIRST_OCCURRENCE = 1;
        public static int KM_USE_LAST_OCCURRENCE = 2;

        /*
         * "Full" processing mode starts by setting all bits to false and only setting bits
         * for documents that contain the given field and are identified as none-duplicates. 

         * "Fast" processing sets all bits to true then unsets all duplicate docs found for the
         * given field. This approach avoids the need to read TermDocs for terms that are seen 
         * to have a document frequency of exactly "1" (i.e. no duplicates). While a potentially 
         * faster approach , the downside is that bitsets produced will include bits set for 
         * documents that do not actually contain the field given.
         * 
         */
        int processingMode = PM_FULL_VALIDATION;
        public static int PM_FULL_VALIDATION = 1;
        public static int PM_FAST_INVALIDATION = 2;



        public DuplicateFilter(String fieldName) :  this(fieldName, KM_USE_LAST_OCCURRENCE, PM_FULL_VALIDATION)
        {
        }


        public DuplicateFilter(String fieldName, int keepMode, int processingMode)
        {
            this.fieldName = fieldName;
            this.keepMode = keepMode;
            this.processingMode = processingMode;
        }

        public override DocIdSet GetDocIdSet(IndexReader reader)
        {
            if (processingMode == PM_FAST_INVALIDATION)
            {
                return FastBits(reader);
            }
            else
            {
                return CorrectBits(reader);
            }
        }

        private OpenBitSet CorrectBits(IndexReader reader)
        {
            OpenBitSet bits = new OpenBitSet(reader.MaxDoc); //assume all are INvalid
            Term startTerm = new Term(fieldName);
            TermEnum te = reader.Terms(startTerm);
            if (te != null)
            {
                Term currTerm = te.Term;
                while ((currTerm != null) && (currTerm.Field == startTerm.Field)) //term fieldnames are interned
                {
                    int lastDoc = -1;
                    //set non duplicates
                    TermDocs td = reader.TermDocs(currTerm);
                    if (td.Next())
                    {
                        if (keepMode == KM_USE_FIRST_OCCURRENCE)
                        {
                            bits.Set(td.Doc);
                        }
                        else
                        {
                            do
                            {
                                lastDoc = td.Doc;
                            } while (td.Next());
                            bits.Set(lastDoc);
                        }
                    }
                    if (!te.Next())
                    {
                        break;
                    }
                    currTerm = te.Term;
                }
            }
            return bits;
        }

        private OpenBitSet FastBits(IndexReader reader)
        {
            OpenBitSet bits = new OpenBitSet(reader.MaxDoc);
            bits.Set(0, reader.MaxDoc); //assume all are valid
            Term startTerm = new Term(fieldName);
            TermEnum te = reader.Terms(startTerm);
            if (te != null)
            {
                Term currTerm = te.Term;

                while ((currTerm != null) && (currTerm.Field == startTerm.Field)) //term fieldnames are interned
                {
                    if (te.DocFreq() > 1)
                    {
                        int lastDoc = -1;
                        //unset potential duplicates
                        TermDocs td = reader.TermDocs(currTerm);
                        td.Next();
                        if (keepMode == KM_USE_FIRST_OCCURRENCE)
                        {
                            td.Next();
                        }
                        do
                        {
                            lastDoc = td.Doc;
                            bits.Clear(lastDoc);
                        } while (td.Next());
                        if (keepMode == KM_USE_LAST_OCCURRENCE)
                        {
                            //restore the last bit
                            bits.Set(lastDoc);
                        }
                    }
                    if (!te.Next())
                    {
                        break;
                    }
                    currTerm = te.Term;
                }
            }
            return bits;
        }

        public string FieldName
        {
            get { return fieldName; }
            set { this.fieldName = value; }
        }

        public int KeepMode
        {
            get { return keepMode; }
            set { this.keepMode = value; }
        }

        public override bool Equals(Object obj)
        {
            if (this == obj)
                return true;
            if ((obj == null) || (obj.GetType()!= this.GetType()))
                return false;
            DuplicateFilter other = (DuplicateFilter)obj;
            return keepMode == other.keepMode &&
            processingMode == other.processingMode &&
                (fieldName == other.fieldName || (fieldName != null && fieldName.Equals(other.fieldName)));
        }

        public override int GetHashCode()
        {
            int hash = 217;
            hash = 31 * hash + keepMode;
            hash = 31 * hash + processingMode;
            hash = 31 * hash + fieldName.GetHashCode();
            return hash;
        }

        public int ProcessingMode
        {
            get { return processingMode; }
            set { this.processingMode = value; }
        }
    }
}
