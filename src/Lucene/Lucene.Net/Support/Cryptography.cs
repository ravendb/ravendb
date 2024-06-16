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

using System.Security.Cryptography;

namespace Lucene.Net.Support
{
    public static class Cryptography
    {
        public static bool FIPSCompliant = false;

        public static HashAlgorithm HashAlgorithm
        {
            get
            {
                if (FIPSCompliant)
                {
                    //LUCENENET-175
                    //No Assumptions should be made on the HashAlgorithm. It may change in time.
                    //SHA256 SHA384 SHA512 etc.
                    return SHA1.Create();
                }
            return MD5.Create();
        }
    }
}
}