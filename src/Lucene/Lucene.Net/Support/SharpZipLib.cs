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
using System.Reflection;

namespace Lucene.Net.Support
{
    public class SharpZipLib
    {
        static System.Reflection.Assembly asm = null;

        static SharpZipLib()
        {
            try
            {
                asm = Assembly.Load(new AssemblyName("ICSharpCode.SharpZipLib"));
            }
            catch { }
        }

        public static Deflater CreateDeflater()
        {
            if (asm == null) throw new System.IO.FileNotFoundException("Can not load ICSharpCode.SharpZipLib.dll");

#if !NETSTANDARD2_1
            return new Deflater(asm.CreateInstance("ICSharpCode.SharpZipLib.Zip.Compression.Deflater"));
#else
            var type = asm.GetType("ICSharpCode.SharpZipLib.Zip.Compression.Deflater");
            return new Deflater(Activator.CreateInstance(type));
#endif
        }

        public static Inflater CreateInflater()
        {
            if (asm == null) throw new System.IO.FileNotFoundException("Can not load ICSharpCode.SharpZipLib.dll");

#if !NETSTANDARD2_1
            return new Inflater(asm.CreateInstance("ICSharpCode.SharpZipLib.Zip.Compression.Inflater"));
#else
            var type = asm.GetType("ICSharpCode.SharpZipLib.Zip.Compression.Inflater");
            return new Inflater(Activator.CreateInstance(type));
#endif
        }
    }
}
