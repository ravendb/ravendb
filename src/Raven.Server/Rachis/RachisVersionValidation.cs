using Raven.Server.ServerWide.Commands;
using Sparrow.Json;

namespace Raven.Server.Rachis
{
    public abstract class RachisVersionValidation
    {
        public abstract void AssertPutCommandToLeader(CommandBase cmd);

        public abstract void AssertEntryBeforeSendToFollower(BlittableJsonReaderObject entry, int version, string follower);
    }
}
