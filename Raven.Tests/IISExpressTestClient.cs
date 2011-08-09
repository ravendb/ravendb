using System;
using Raven.Client;

namespace Raven.Tests
{
    class IISExpressTestClient : IISClientTestBase, IDisposable
    {
        private ProcessDriver _iisExpress;

        public override IDocumentStore GetDocumentStore()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}