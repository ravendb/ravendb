using System;
using Raven.Embedded;

namespace Raven.TestDriver
{
    public sealed class TestServerOptions : ServerOptions
    {
        public TestServerOptions()
        {
            License.ThrowOnInvalidOrMissingLicense = true;
        }

        public static TestServerOptions UseFiddler()
        {
            return new TestServerOptions
            {
                ServerUrl = $"http://{Environment.MachineName}:0",
                CommandLineArgs =
                {
                    "--Security.UnsecuredAccessAllowed=PrivateNetwork"
                }
            };
        }
    }
}
