using System;
using System.IO;
using Raven.Client.ServerWide.Tcp;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.ServerWide.Tcp.Sync
{
    public sealed class TcpNegotiateParameters : AbstractTcpNegotiateParameters
    {
        /// <summary>
        /// ReadResponseAndGetVersion Function should take care reading the TcpConnectionHeaderResponse respond from the input 'stream'
        /// And return the version of the supported TCP protocol.
        ///
        /// If the respond is 'Drop' the function should throw.
        /// If the respond is 'None' the function should throw.
        /// If the respond is 'TcpMismatch' the function should return the read version.
        /// </summary>
        public Func<JsonOperationContext, BlittableJsonTextWriter, Stream, string, int> ReadResponseAndGetVersionCallback { get; set; }
    }
}
