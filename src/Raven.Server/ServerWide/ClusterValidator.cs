﻿using System;
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
                throw new RejectPutClusterCommandException($"Cannot accept the command '{commandName}', " +
                                                          $"because the cluster version is '{ClusterCommandsVersionManager.CurrentClusterMinimalVersion}', " +
                                                          $"while this command can be applied in cluster with minimum version of {ClusterCommandsVersionManager.ClusterCommandsVersions[commandName]}");
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
                throw new RejectSendToFollowerException("Rachis entry is invalid!");
            }

            var myCommandVersion = ClusterCommandsVersionManager.ClusterCommandsVersions[type];

            if (myCommandVersion > version)
            {
                throw new RejectSendToFollowerException($"The command '{type}' with the version {myCommandVersion} is not supported on follower {follower}.");
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
    }
}
