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
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Lucene.Net.Search
{
    public static class Extensions
    {
        //CartesianProduct - Lambda
        public static IEnumerable<IEnumerable<T>> CartesianProduct<T>(this IEnumerable<IList<T>> sequences)
        {
            IEnumerable<IEnumerable<T>> emptyProduct = new IEnumerable<T>[] { Enumerable.Empty<T>() };
            return sequences.Aggregate(
                emptyProduct,
                (accumulator, sequence) =>
                {
                    return accumulator.SelectMany(
                        (accseq => sequence),
                        (accseq, item) => accseq.Concat(new T[] { item })
                    );
                }
            );
        }

        //CartesianProduct - Lambda
        public static IEnumerable<IEnumerable<T>> CartesianProduct<T>(this IEnumerable<IEnumerable<T>> sequences)
        {
            IEnumerable<IEnumerable<T>> emptyProduct = new IEnumerable<T>[] { Enumerable.Empty<T>() };
            return sequences.Aggregate(
                emptyProduct,
                (accumulator, sequence) =>
                {
                    return accumulator.SelectMany(
                        (accseq => sequence),
                        (accseq, item) => accseq.Concat(new T[] { item })
                    );
                }
            );
        }

        //CartesianProduct - LINQ
        //http://blogs.msdn.com/b/ericlippert/archive/2010/06/28/computing-a-cartesian-product-with-linq.aspx
        static IEnumerable<IEnumerable<T>> CartesianProduct2<T>(this IEnumerable<IEnumerable<T>> sequences)
        {
            IEnumerable<IEnumerable<T>> emptyProduct = new[] { Enumerable.Empty<T>() };
            return sequences.Aggregate(
                emptyProduct,
                (accumulator, sequence) =>
                    from accseq in accumulator
                    from item in sequence
                    select accseq.Concat(new[] { item }));
        }
    }
}
