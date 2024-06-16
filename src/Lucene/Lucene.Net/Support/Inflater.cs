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

namespace Lucene.Net.Support
{
    using Lucene.Net.Util;

    public class Inflater
    {
        delegate void SetInputDelegate(byte[] buffer);
        delegate bool GetIsFinishedDelegate();
        delegate int InflateDelegate(byte[] buffer);

        SetInputDelegate setInputMethod;
        GetIsFinishedDelegate getIsFinishedMethod;
        InflateDelegate inflateMethod;

        internal Inflater(object inflaterInstance)
        {
            Type type = inflaterInstance.GetType();

            setInputMethod = (SetInputDelegate)Delegate.CreateDelegate(
                typeof(SetInputDelegate),
                inflaterInstance,
                type.GetMethod("SetInput", new Type[] { typeof(byte[]) }));

            getIsFinishedMethod = (GetIsFinishedDelegate)Delegate.CreateDelegate(
                typeof(GetIsFinishedDelegate),
                inflaterInstance,
                type.GetMethod("get_IsFinished", Type.EmptyTypes));

            inflateMethod = (InflateDelegate)Delegate.CreateDelegate(
                typeof(InflateDelegate),
                inflaterInstance,
                type.GetMethod("Inflate", new Type[] { typeof(byte[]) }));
        }

        public void SetInput(byte[] buffer)
        {
            setInputMethod(buffer);
        }

        public bool IsFinished
        {
            get { return getIsFinishedMethod(); }
        }

        public int Inflate(byte[] buffer)
        {
            return inflateMethod(buffer);
        }
    }
}
