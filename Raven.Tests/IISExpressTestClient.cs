using System;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Util;

namespace Raven.Tests
{
    class IISExpressTestClient : IISClientTestBase, IDisposable
    {
        public static int Port = 8084;

        private IISExpressDriver _iisExpress;

        public override IDocumentStore GetDocumentStore()
        {
            if (_iisExpress == null)
            {
                _iisExpress = new IISExpressDriver();

                _iisExpress.Start(DeployWebProjectToTestDirectory(), 8084);
            }

            return new DocumentStore()
            {
                Url = _iisExpress.Url
            };
        }

        public void Dispose()
        {
            if (_iisExpress != null)
            {
                _iisExpress.Dispose();
                _iisExpress = null;
            }
        }
    }
}