using System.Collections.Generic;

namespace Raven.Embedded
{
    public class ServerOptions
    {
        public string FrameworkVersion { get; set; } = "2.1.0";

        public string DataDir { get; set; }

        public List<string> CommandLineArgs { get; set; } = new List<string>
        {
            "--ServerUrl=http://127.0.0.1:0",
            "--RunInMemory=false",
            "--Setup.Mode=None"
        };

        public static ServerOptions Default = new ServerOptions();

    }
}
