using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class DeleteCertificateFromClusterCommand : DeleteValueCommand
    {
        public DeleteCertificateFromClusterCommand() { }
        public DeleteCertificateFromClusterCommand(string raftRequestId) : base(raftRequestId) { }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            using (context.OpenReadTransaction())
            {
                var read = store.Cluster.GetCertificateByThumbprint(context, Name);
                if (read == null)
                    return;

                var definition = JsonDeserializationServer.CertificateDefinition(read);
                if (definition.SecurityClearance != SecurityClearance.ClusterAdmin || definition.SecurityClearance != SecurityClearance.ClusterNode)
                    return;
            }

            AssertClusterAdmin(isClusterAdmin);
        }
    }
}
