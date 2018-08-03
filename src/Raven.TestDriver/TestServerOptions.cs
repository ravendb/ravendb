using System;
using Raven.Embedded;

namespace Raven.TestDriver
{
    public class TestServerOptions : ServerOptions
    {
        public new string ServerDirectory
        {
            get => base.ServerDirectory;
            set => base.ServerDirectory = value;
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
