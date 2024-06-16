/**
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

namespace Lucene.Net.Analysis
{

    ///<summary>
    ///* <p>
    /// * Allows multiple {@link Filter}s to be chained.
    /// * Logical operations such as <b>NOT</b> and <b>XOR</b>
    /// * are applied between filters. One operation can be used
    /// * for all filters, or a specific operation can be declared
    /// * for each filter.
    /// * </p>
    /// * <p>
    /// * Order in which filters are called depends on
    /// * the position of the filter in the chain. It's probably
    /// * more efficient to place the most restrictive filters
    /// * /least computationally-intensive filters first.
    /// * </p>
    ///</summary>
    public class ChainedFilter : Filter
    {
        public enum Logic
        {
            NONE = -1,
            OR = 0,
            AND = 1,
            ANDNOT = 2,
            XOR = 3
        };

        ///<summary>Logical operation when none is declared. Defaults to OR</summary>
        public const Logic DEFAULT = Logic.OR;

        /** The filter chain */
        private Filter[] chain = null;

        private Logic[] logicArray;

        private Logic logic = Logic.NONE;

        ///<summary>Ctor</summary><param name="chain">The chain of filters</param>
        public ChainedFilter(Filter[] chain)
        {
            this.chain = chain;
        }

        ///<summary>ctor</summary>
        ///<param name="chain">The chain of filters</param>
        ///<param name="logicArray">Logical operations to apply between filters</param>
        public ChainedFilter(Filter[] chain, Logic[] logicArray)
        {
            this.chain = chain;
            this.logicArray = logicArray;
        }

        ///<summary>ctor</summary>
        ///<param name="chain">The chain of filters</param>
        ///<param name="logic">Logical operation to apply to ALL filters</param>
        public ChainedFilter(Filter[] chain, Logic logic)
        {
            this.chain = chain;
            this.logic = logic;
        }

        ///<see cref="Filter#getDocIdSet"/>
        public override DocIdSet GetDocIdSet(IndexReader reader)
        {
            int[] index = new int[1]; // use array as reference to modifiable int; 
            index[0] = 0;             // an object attribute would not be thread safe.
            if (logic != Logic.NONE)
                return GetDocIdSet(reader, logic, index);
            else if (logicArray != null)
                return GetDocIdSet(reader, logicArray, index);
            else
                return GetDocIdSet(reader, DEFAULT, index);
        }

        private DocIdSetIterator GetDISI(Filter filter, IndexReader reader)
        {
            DocIdSet docIdSet = filter.GetDocIdSet(reader);
            if (docIdSet == null)
            {
                return DocIdSet.EMPTY_DOCIDSET.Iterator();
            }
            else
            {
                DocIdSetIterator iter = docIdSet.Iterator();
                if (iter == null)
                {
                    return DocIdSet.EMPTY_DOCIDSET.Iterator();
                }
                else
                {
                    return iter;
                }
            }
        }

        private OpenBitSetDISI InitialResult(IndexReader reader, Logic logic, int[] index)
        {
            OpenBitSetDISI result;
            /**
             * First AND operation takes place against a completely false
             * bitset and will always return zero results.
             */
            if (logic == Logic.AND)
            {
                result = new OpenBitSetDISI(GetDISI(chain[index[0]], reader), reader.MaxDoc());
                ++index[0];
            }
            else if (logic == Logic.ANDNOT)
            {
                result = new OpenBitSetDISI(GetDISI(chain[index[0]], reader), reader.MaxDoc());
                result.Flip(0, reader.MaxDoc()); // NOTE: may set bits for deleted docs.
                ++index[0];
            }
            else
            {
                result = new OpenBitSetDISI(reader.MaxDoc());
            }
            return result;
        }


        ///<summary>
        ///  * Provide a SortedVIntList when it is definitely
        ///  * smaller than an OpenBitSet
        ///  * @deprecated Either use CachingWrapperFilter, or
        ///  * switch to a different DocIdSet implementation yourself.
        ///  * This method will be removed in Lucene 4.0 
        ///</summary>
        protected DocIdSet FinalResult(OpenBitSetDISI result, int maxDocs)
        {
            return result;
        }


        /**
         * Delegates to each filter in the chain.
         * @param reader IndexReader
         * @param logic Logical operation
         * @return DocIdSet
         */
        private DocIdSet GetDocIdSet(IndexReader reader, Logic logic, int[] index)
        {
            OpenBitSetDISI result = InitialResult(reader, logic, index);
            for (; index[0] < chain.Length; index[0]++)
            {
                DoChain(result, logic, chain[index[0]].GetDocIdSet(reader));
            }
            return FinalResult(result, reader.MaxDoc());
        }

        /**
         * Delegates to each filter in the chain.
         * @param reader IndexReader
         * @param logic Logical operation
         * @return DocIdSet
         */
        private DocIdSet GetDocIdSet(IndexReader reader, Logic[] logic, int[] index)
        {
            if (logic.Length != chain.Length)
                throw new ArgumentException("Invalid number of elements in logic array");

            OpenBitSetDISI result = InitialResult(reader, logic[0], index);
            for (; index[0] < chain.Length; index[0]++)
            {
                DoChain(result, logic[index[0]], chain[index[0]].GetDocIdSet(reader));
            }
            return FinalResult(result, reader.MaxDoc());
        }

        public override String ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("ChainedFilter: [");
            for (int i = 0; i < chain.Length; i++)
            {
                sb.Append(chain[i]);
                sb.Append(' ');
            }
            sb.Append(']');
            return sb.ToString();
        }

        private void DoChain(OpenBitSetDISI result, Logic logic, DocIdSet dis)
        {

            if (dis is OpenBitSet)
            {
                // optimized case for OpenBitSets
                switch (logic)
                {
                    case Logic.OR:
                        result.Or((OpenBitSet)dis);
                        break;
                    case Logic.AND:
                        result.And((OpenBitSet)dis);
                        break;
                    case Logic.ANDNOT:
                        result.AndNot((OpenBitSet)dis);
                        break;
                    case Logic.XOR:
                        result.Xor((OpenBitSet)dis);
                        break;
                    default:
                        DoChain(result, DEFAULT, dis);
                        break;
                }
            }
            else
            {
                DocIdSetIterator disi;
                if (dis == null)
                {
                    disi = DocIdSet.EMPTY_DOCIDSET.Iterator();
                }
                else
                {
                    disi = dis.Iterator();
                    if (disi == null)
                    {
                        disi = DocIdSet.EMPTY_DOCIDSET.Iterator();
                    }
                }

                switch (logic)
                {
                    case Logic.OR:
                        result.InPlaceOr(disi);
                        break;
                    case Logic.AND:
                        result.InPlaceAnd(disi);
                        break;
                    case Logic.ANDNOT:
                        result.InPlaceNot(disi);
                        break;
                    case Logic.XOR:
                        result.InPlaceXor(disi);
                        break;
                    default:
                        DoChain(result, DEFAULT, dis);
                        break;
                }
            }
        }

    }

}