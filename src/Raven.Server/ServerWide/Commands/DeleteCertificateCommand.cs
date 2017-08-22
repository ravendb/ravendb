using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Commands
{
    public class DeleteCertificateCommand : DeleteValueCommand
    {
        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            using (context.OpenReadTransaction())
            {
                var read = store.Cluster.Read(context, Name);
                if (read == null)
                    return;
                var definition = JsonDeserializationServer.CertificateDefinition(read);
                if (definition.Clearance != SecurityClearance.ClusterAdmin)
                    return;
            }

            AssertClusterAdmin(isClusterAdmin);
        }
    }
}
