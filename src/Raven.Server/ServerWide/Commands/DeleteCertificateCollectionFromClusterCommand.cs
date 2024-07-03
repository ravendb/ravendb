using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class DeleteCertificateCollectionFromClusterCommand : DeleteMultipleValuesCommand
    {
        public DeleteCertificateCollectionFromClusterCommand() { }
        public DeleteCertificateCollectionFromClusterCommand(string requestId) : base(requestId)
        {
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            using (context.OpenReadTransaction())
            {
                foreach (var name in Names)
                {
                    var read = store.Cluster.GetCertificateByThumbprint(context, name);
                    if (read == null)
                        return;

                    var definition = JsonDeserializationServer.CertificateDefinition(read);
                    if (definition.SecurityClearance != SecurityClearance.ClusterAdmin || definition.SecurityClearance != SecurityClearance.ClusterNode)
                        return;
                }
            }

            AssertClusterAdmin(isClusterAdmin);
        }

        public override void AfterDelete(ServerStore store, ClusterOperationContext context)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += tx =>
            {
                if (tx.Committed)
                {
                    store.Server.Statistics.RemoveLastAuthorizedCertificateRequestTime(Names);
                }
            };
        }
    }
}
