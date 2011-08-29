using System;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Util;

namespace Raven.Tests
{
    public class IISExpressTestClient : IDisposable
    {
        public static int Port = 8084;

        private IISExpressDriver _iisExpress;

        public IDocumentStore NewDocumentStore()
        {
            if (_iisExpress == null)
            {
                _iisExpress = new IISExpressDriver();

                _iisExpress.Start(IISDeploymentUtil.DeployWebProjectToTestDirectory(), 8084);
            }

            return new DocumentStore()
            {
                Url = _iisExpress.Url
            }.Initialize();
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