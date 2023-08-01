using System;
using System.Diagnostics.CodeAnalysis;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json;

namespace Raven.Server.ServerWide
{
    public sealed class ClusterValidator : RachisVersionValidation
    {
        public ClusterValidator([NotNull] ClusterCommandsVersionManager commandsVersionManager)
            : base(commandsVersionManager)
        {
        }

        public override void AssertPutCommandToLeader(CommandBase cmd)
        {
            var commandName = cmd.GetType().Name;
            if (CommandsVersionManager.CanPutCommand(commandName) == false)
            {
                RejectPutClusterCommandException.Throw($"Cannot accept the command '{commandName}', " +
                                                          $"because the cluster version is '{CommandsVersionManager.CurrentClusterMinimalVersion}', " +
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

            var myCommandVersion = ClusterCommandsVersionManager.ClusterCommandsVersions[type];

            if (myCommandVersion > version)
            {
                RejectSendToFollowerException.Throw($"The command '{type}' with the version {myCommandVersion} is not supported on follower {follower} with version build {version}.");
            }
        }
    }

    public sealed class RejectPutClusterCommandException : Exception
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

        [DoesNotReturn]
        public static void Throw(string msg)
        {
            throw new RejectPutClusterCommandException(msg);
        }
    }

    public sealed class RejectSendToFollowerException : Exception
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

        [DoesNotReturn]
        public static void Throw(string msg)
        {
            throw new RejectSendToFollowerException(msg);
        }
    }
}
