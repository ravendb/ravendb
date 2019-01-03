using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Commands
{
    public class DeleteCertificateCollectionFromClusterCommand : DeleteMultipleValuesCommand
    {
        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            using (context.OpenReadTransaction())
            {
                foreach (var name in Names)
                {
                    var read = store.Cluster.Read(context, name);
                    if (read == null)
                        return;

                    var definition = JsonDeserializationServer.CertificateDefinition(read);
                    if (definition.SecurityClearance != SecurityClearance.ClusterAdmin || definition.SecurityClearance != SecurityClearance.ClusterNode)
                        return;
                }
            }

            AssertClusterAdmin(isClusterAdmin);
        }
    }
}
