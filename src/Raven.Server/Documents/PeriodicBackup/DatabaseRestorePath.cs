using Raven.Server.Web.System;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class DatabaseRestorePath
    {
        public ConnectionType ConnectionType { get; set; }

        public string Path { get; set; }

        public string EncodedCredentials { get; set; }
    }
}
