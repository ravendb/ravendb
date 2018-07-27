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
    }
}
