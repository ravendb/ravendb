using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{
    public class OpenFilesSessionOptions
    {
        public string FileSystem { get; set; }
        public ICredentials Credentials { get; set; }
        public string ApiKey { get; set; }
    }
}
