using System;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json;

namespace Raven.Server.Rachis
{
    public abstract class RachisVersionValidation
    {
        protected readonly ClusterCommandsVersionManager CommandsVersionManager;

        protected RachisVersionValidation([NotNull] ClusterCommandsVersionManager commandsVersionManager)
        {
            CommandsVersionManager = commandsVersionManager ?? throw new ArgumentNullException(nameof(commandsVersionManager));
        }

        public abstract void AssertPutCommandToLeader(CommandBase cmd);

        public abstract void AssertEntryBeforeSendToFollower(BlittableJsonReaderObject entry, int version, string follower);
    }
}
