using System;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json;

namespace Raven.Server.ServerWide
{
    public class ClusterValidator : RachisVersionValidation
    {
        public override void AssertPutCommandToLeader(CommandBase cmd)
        {
            var commandName = cmd.GetType().Name;
            if (ClusterCommandsVersionManager.CanPutCommand(commandName) == false)
            {
                RejectPutClusterCommandException.Throw($"Cannot accept the command '{commandName}', " +
                                                          $"because the cluster version is '{ClusterCommandsVersionManager.CurrentClusterMinimalVersion}', " +
                                                          $"while this command can be applied in cluster with minimum version of {ClusterCommandsVersionManager.ClusterCommandsVersions[commandName]}");
            }

            if (cmd.UniqueRequestId == null)
            {
               RejectPutClusterCommandException.Throw($"The command {commandName} doesn't contain the raft request unique identifier.");
            }
        }

        public override void AssertEntryBeforeSendToFollower(BlittableJsonReaderObject entry, int version, string follower)
        {
            if (entry.TryGet(nameof(RachisEntry.Flags), out RachisEntryFlags flag) && flag != RachisEntryFlags.StateMachineCommand)
            {
                return;
            }

            string type = null;
            if ((entry.TryGet(nameof(RachisEntry.Entry), out BlittableJsonReaderObject blittableEntry) &&
                 blittableEntry.TryGet("Type", out type)) == false)
            {
                RejectSendToFollowerException.Throw("Rachis entry has no type!");
            }

            if (ClusterCommandsVersionManager.ClusterCommandsVersions.TryGetValue(type, out var myCommandVersion) == false || 
                myCommandVersion > version)
            {
                RejectSendToFollowerException.Throw($"The command '{type}' {(myCommandVersion > 0 ? $"with the version {myCommandVersion} " : null)}" +
                                                    $"is not supported on follower {follower} with version build {version}.");
            }
        }

        
    }

    public class RejectPutClusterCommandException : Exception
    {
        public RejectPutClusterCommandException()
        {
        }

        public RejectPutClusterCommandException(string message) : base(message)
        {
        }

        public RejectPutClusterCommandException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public static void Throw(string msg)
        {
            throw new RejectPutClusterCommandException(msg);
        }
    }

    public class RejectSendToFollowerException : Exception
    {
        public RejectSendToFollowerException()
        {
        }

        public RejectSendToFollowerException(string message) : base(message)
        {
        }

        public RejectSendToFollowerException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public static void Throw(string msg)
        {
            throw new RejectSendToFollowerException(msg);
        }
    }
}
