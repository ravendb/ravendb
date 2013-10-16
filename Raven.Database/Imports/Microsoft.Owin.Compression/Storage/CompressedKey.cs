// <copyright file="CompressedKey.cs" company="Microsoft Open Technologies, Inc.">
// Copyright 2011-2013 Microsoft Open Technologies, Inc. All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>

using System;
using System.Collections.Generic;

namespace Microsoft.Owin.Compression.Storage
{
    public struct CompressedKey : IEquatable<CompressedKey>
    {
        private static readonly IEqualityComparer<CompressedKey> CompressedKeyComparerInstance = new CompressedKeyEqualityComparer();

        /// <summary>
        /// Initializes a new instance of the <see cref="T:System.Object"/> class.
        /// </summary>
        public CompressedKey(string etag, string requestPath, string requestQueryString, string requestMethod)
            : this()
        {
            ETag = etag;
            RequestPath = requestPath;
            RequestQueryString = requestQueryString;
            RequestMethod = requestMethod;
        }

        // TODO: should storage key vary by less-than this info?
        // should static file middleware kill querystring to improve hit efficiency?
        public string ETag { get; set; }
        public string RequestPath { get; set; }
        public string RequestQueryString { get; set; }
        public string RequestMethod { get; set; }

        #region Equality members

        public bool Equals(CompressedKey other)
        {
            return string.Equals(ETag, other.ETag) &&
                string.Equals(RequestPath, other.RequestPath) &&
                string.Equals(RequestQueryString, other.RequestQueryString) &&
                string.Equals(RequestMethod, other.RequestMethod);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }
            return obj is CompressedKey && Equals((CompressedKey)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = (ETag != null ? ETag.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (RequestPath != null ? RequestPath.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (RequestQueryString != null ? RequestQueryString.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (RequestMethod != null ? RequestMethod.GetHashCode() : 0);
                return hashCode;
            }
        }

        public static bool operator ==(CompressedKey left, CompressedKey right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(CompressedKey left, CompressedKey right)
        {
            return !left.Equals(right);
        }

        #endregion

        #region CompressedKeyEqualityComparer

        private sealed class CompressedKeyEqualityComparer : IEqualityComparer<CompressedKey>
        {
            public bool Equals(CompressedKey x, CompressedKey y)
            {
                return string.Equals(x.ETag, y.ETag) &&
                    string.Equals(x.RequestPath, y.RequestPath) &&
                    string.Equals(x.RequestQueryString, y.RequestQueryString) &&
                    string.Equals(x.RequestMethod, y.RequestMethod);
            }

            public int GetHashCode(CompressedKey obj)
            {
                unchecked
                {
                    int hashCode = (obj.ETag != null ? obj.ETag.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (obj.RequestPath != null ? obj.RequestPath.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (obj.RequestQueryString != null ? obj.RequestQueryString.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (obj.RequestMethod != null ? obj.RequestMethod.GetHashCode() : 0);
                    return hashCode;
                }
            }
        }

        public static IEqualityComparer<CompressedKey> CompressedKeyComparer
        {
            get { return CompressedKeyComparerInstance; }
        }

        #endregion
    }
}
