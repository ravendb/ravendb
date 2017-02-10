using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.Data
{
    public class TcpConnectionHeaderResponse
    {
        public enum AuthorizationStatus
        {
            PreconditionFailed,
            Forbidden,
            Success
        }

        public AuthorizationStatus Status { get; set; }

    }
}
