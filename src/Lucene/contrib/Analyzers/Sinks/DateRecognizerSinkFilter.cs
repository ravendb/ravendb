/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Util;

namespace Lucene.Net.Analysis.Sinks
{
    /*
  * Attempts to parse the {@link org.apache.lucene.analysis.Token#termBuffer()} as a Date using a <see cref="System.IFormatProvider"/>.
  * If the value is a Date, it will add it to the sink.
  * <p/> 
  *
  **/
    public class DateRecognizerSinkFilter : TeeSinkTokenFilter.SinkFilter
    {
        public const string DATE_TYPE = "date";

        protected IFormatProvider dateFormat;
        protected ITermAttribute termAtt;

        /*
         * Uses <see cref="System.Globalization.CultureInfo.CurrentCulture.DateTimeFormatInfo"/> as the <see cref="IFormatProvider"/> object.
         */
        public DateRecognizerSinkFilter()
            : this(System.Globalization.CultureInfo.CurrentCulture)
        {

        }

        public DateRecognizerSinkFilter(IFormatProvider dateFormat)
        {
            this.dateFormat = dateFormat;
        }

        public override bool Accept(AttributeSource source)
        {
            if (termAtt == null)
            {
                termAtt = source.AddAttribute<ITermAttribute>();
            }
            try
            {
                DateTime date = DateTime.Parse(termAtt.Term, dateFormat);//We don't care about the date, just that we can parse it as a date
                if (date != null)
                {
                    return true;
                }
            }
            catch (FormatException)
            {

            }

            return false;
        }

    }
}
