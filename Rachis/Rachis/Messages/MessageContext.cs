using System;
using System.IO;

namespace Rachis.Messages
{
    public abstract class MessageContext
    {
        public object Message { get; set; }
	    public Stream Stream { get; set; }
        public bool AsyncResponse { get; set; }

        public abstract void Reply(CanInstallSnapshotResponse resp);
	    public abstract void Reply(InstallSnapshotResponse resp);
		public abstract void Reply(AppendEntriesResponse resp);
		public abstract void Reply(RequestVoteResponse resp);

		public abstract void ExecuteInEventLoop(Action action);

	    public abstract void Done();
	    public abstract void Error(Exception exception);
    }
}