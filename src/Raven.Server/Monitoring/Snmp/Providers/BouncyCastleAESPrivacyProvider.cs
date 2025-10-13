// Bouncy Castle AES privacy provider
// Copyright (C) 2009-2010 Lex Li, Milan Sinadinovic
// Copyright (C) 2018 Matt Zinkevicius
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy of this
// software and associated documentation files (the "Software"), to deal in the Software
// without restriction, including without limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
// to whom the Software is furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all copies or
// substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
// INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
// PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
// FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Security;

namespace Raven.Server.Monitoring.Snmp.Providers
{
    /// <summary>
    /// Privacy provider for AES 128.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "AES", Justification = "definition")]
    public sealed class BouncyCastleAESPrivacyProvider : BouncyCastleAESPrivacyProviderBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BouncyCastleAESPrivacyProvider"/> class.
        /// </summary>
        /// <param name="phrase">The phrase.</param>
        /// <param name="auth">The authentication provider.</param>
        public BouncyCastleAESPrivacyProvider(OctetString phrase, IAuthenticationProvider auth)
            : base(16, phrase, auth)
        { }

        /// <summary>
        /// Returns a string that represents this object.
        /// </summary>
        public override string ToString() => "AES 128 (BouncyCastle) privacy provider";
    }
}
