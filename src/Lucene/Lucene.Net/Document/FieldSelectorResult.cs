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

using System.Runtime.InteropServices;

namespace Lucene.Net.Documents
{
	/// <summary>Provides information about what should be done with this Field</summary>
    public enum FieldSelectorResult
    {
        /// <summary>
        /// 
        /// </summary>
        INVALID, // TODO: This is kinda a kludgy workaround for the fact enums can't be null 

        /// <summary> Load this <see cref="Field" /> every time the <see cref="Document" /> is loaded, reading in the data as it is encountered.
        /// <see cref="Document.GetField(String)" /> and <see cref="Document.GetFieldable(String)" /> should not return null.
        /// <p/>
        /// <see cref="Document.Add(IFieldable)" /> should be called by the Reader.
        /// </summary>
        LOAD,

        /// <summary> Lazily load this <see cref="Field" />.  This means the <see cref="Field" /> is valid, but it may not actually contain its data until
        /// invoked.  <see cref="Document.GetField(String)" /> SHOULD NOT BE USED.  <see cref="Document.GetFieldable(String)" /> is safe to use and should
        /// return a valid instance of a <see cref="IFieldable" />.
        /// <p/>
        /// <see cref="Document.Add(IFieldable)" /> should be called by the Reader.
        /// </summary>
        LAZY_LOAD,

        /// <summary> Do not load the <see cref="Field" />.  <see cref="Document.GetField(String)" /> and <see cref="Document.GetFieldable(String)" /> should return null.
        /// <see cref="Document.Add(IFieldable)" /> is not called.
        /// <p/>
        /// <see cref="Document.Add(IFieldable)" /> should not be called by the Reader.
        /// </summary>
        NO_LOAD,

        /// <summary> Load this field as in the <see cref="LOAD" /> case, but immediately return from <see cref="Field" /> loading for the <see cref="Document" />.  Thus, the
        /// Document may not have its complete set of Fields.  <see cref="Document.GetField(String)" /> and <see cref="Document.GetFieldable(String)" /> should
        /// both be valid for this <see cref="Field" />
        /// <p/>
        /// <see cref="Document.Add(IFieldable)" /> should be called by the Reader.
        /// </summary>
        LOAD_AND_BREAK,

        /// <summary>Expert:  Load the size of this <see cref="Field" /> rather than its value.
        /// Size is measured as number of bytes required to store the field == bytes for a binary or any compressed value, and 2*chars for a String value.
        /// The size is stored as a binary value, represented as an int in a byte[], with the higher order byte first in [0]
        /// </summary>
        SIZE,

        /// <summary>Expert: Like <see cref="SIZE" /> but immediately break from the field loading loop, i.e., stop loading further fields, after the size is loaded </summary>         
        SIZE_AND_BREAK
    }
}