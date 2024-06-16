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

namespace Lucene.Net.Analysis.Shingle.Matrix
{
    public class Column
    {
        public Column(Token token, Analysis.Shingle.Matrix.Matrix enclosingInstance)
            : this(enclosingInstance)
        {
            var row = new Row(this);
            row.Tokens.AddLast(token);
        }

        public Column(Analysis.Shingle.Matrix.Matrix enclosingInstance)
        {
            Rows = new List<Row>();
            Matrix = enclosingInstance;

            lock (Matrix)
            {
                if (Matrix.Columns.Count == 0)
                    IsFirst = true;
            }
            Matrix.Columns.Add(this);
        }

        public Matrix Matrix { get; private set; }

        public List<Row> Rows { get; private set; }

        public int Index
        {
            get { return Matrix.Columns.IndexOf(this); }
        }

        public bool IsFirst { get; set; }

        public bool IsLast { get; set; }

        public override String ToString()
        {
            return "Column{" +
                   "first=" + IsFirst +
                   ", last=" + IsLast +
                   ", rows=" + Rows +
                   '}';
        }
    }
}