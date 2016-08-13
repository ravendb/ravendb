using System;
using System.Collections.Generic;
using System.Net.Sockets;
using Sparrow.Json;

namespace Raven.Server.Documents.TcpHandlers
{
    public class TcpConnectionParams : IDisposable
    {
        public DocumentDatabase DocumentDatabase;

        public JsonOperationContext Context;

        public NetworkStream Stream;

        public TcpClient TcpClient;

        public JsonOperationContext.MultiDocumentParser MultiDocumentParser;

        public List<IDisposable> DisposeOnConnectionClose = new List<IDisposable>();

        public void Dispose()
        {

            foreach (var disposable in DisposeOnConnectionClose)
            {
                try
                {
                    disposable?.Dispose();
                }
                catch (Exception)
                {
                    // nothing to do here
                }
            }
        }
    }
}