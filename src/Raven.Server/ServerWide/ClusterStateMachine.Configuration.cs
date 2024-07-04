using System;
using Raven.Client;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.ServerWide;

public sealed partial class ClusterStateMachine
{
    private void PutClientConfiguration(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index)
    {
        Exception exception = null;
        PutClientConfigurationCommand command = null;
        try
        {
            command = (PutClientConfigurationCommand)JsonDeserializationCluster.Commands[type](cmd);
            if (command.Name.StartsWith(Constants.Documents.Prefix))
                throw new RachisApplyException("Cannot set " + command.Name + " using PutValueCommand, only via dedicated database calls");

            command.UpdateValue(context, index);

            using (var rec = context.ReadObject(command.ValueToJson(), "inner-val"))
                PutValueDirectly(context, command.Name, rec, index);

            var stateRecord = (ClusterStateRecord)context.Transaction.InnerTransaction.LowLevelTransaction.CurrentStateRecord.ClientState;
            context.Transaction.InnerTransaction.LowLevelTransaction.UpdateClientState(stateRecord with
            {
                DefaultIdentityPartsSeparator = ServerStore.LoadDefaultIdentityPartsSeparator(command.Value)
            });
        }
        catch (Exception e)
        {
            exception = e;
            throw;
        }
        finally
        {
            LogCommand(type, index, exception, command);
            NotifyValueChanged(context, type, index);
        }
    }
}
