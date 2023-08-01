using System.Collections.Generic;

namespace Raven.Client.Documents.Commands
{
    public sealed class ClaimDomainResult
    {
        public string Email { get; set; }
        public string[] Emails { get; set; }
        public Dictionary<string, List<string>> Domains { get; set; }
        public string[] RootDomains { get; set; }
    }

    public sealed class ForceRenewResult
    {
        public bool Success { get; set; }
    }
}
