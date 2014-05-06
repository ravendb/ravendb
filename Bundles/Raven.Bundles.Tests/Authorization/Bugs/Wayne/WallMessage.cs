extern alias client;
using System.Collections.Generic;
using client::Raven.Bundles.Authorization.Model;

namespace Raven.Bundles.Tests.Authorization.Bugs.Wayne
{
    public class WallMessage<T>
    {
        public string Id { get; set; }
        public AuthorizationUser Creator { get; set; }
        public T Recipient { get; set; }
        public string MessageBody { get; set; }
        public List<DocumentPermission> Permissions { get; set; }
    }
}