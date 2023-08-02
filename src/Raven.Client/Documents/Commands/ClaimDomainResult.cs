using System.Collections.Generic;

namespace Raven.Client.Documents.Commands
{
    internal sealed class ClaimDomainResult
    {
        public string Email { get; set; }
        public string[] Emails { get; set; }
        public Dictionary<string, List<string>> Domains { get; set; }
        public string[] RootDomains { get; set; }
    }

    internal sealed class ForceRenewResult
    {
        public bool Success { get; set; }
    }
}
