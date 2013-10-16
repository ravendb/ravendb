// <copyright file="SwitchingStream.cs" company="Microsoft Open Technologies, Inc.">
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

using System.IO;
using System.Threading;

namespace Microsoft.Owin.Compression.Infrastructure
{
    public class SwitchingStream : DelegatingStream
    {
        private readonly StaticCompressionContext _compressingContext;
        private readonly Stream _originalBody;

        private Stream _targetStream;
        private bool _targetStreamInitialized;
        private object _targetStreamLock = new object();

        internal SwitchingStream(StaticCompressionContext compressingContext, Stream originalBody)
        {
            _compressingContext = compressingContext;
            _originalBody = originalBody;
        }

        protected override Stream TargetStream
        {
            get
            {
                return LazyInitializer.EnsureInitialized(
                    ref _targetStream,
                    ref _targetStreamInitialized,
                    ref _targetStreamLock,
                    _compressingContext.GetTargetStream);
            }
        }
    }
}
