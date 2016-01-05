using System;
using System.Text;

namespace Raven.Server.ServerWide
{
    public class ServerConfig
    {
        public ServerConfig()
        {
            Port = 8080;
            Path = System.IO.Path.Combine(AppContext.BaseDirectory, "Databases/System");
        }

        public int Port { get; set; }
        
        public string Path { get; set; }

        public bool RunInMemory { get; set; }

        public string Print()
        {
            var sb = new StringBuilder();

            sb.Append("Listening on port: ").Append(Port).Append(". ");
            if (RunInMemory)
            {
                sb.Append("Running in memory.");
            }
            else
            {
                sb.Append("System path is: ").Append(Path).Append(".");
            }

            return sb.ToString();
        }
    }
}