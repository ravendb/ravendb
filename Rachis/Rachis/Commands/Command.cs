using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Rachis.Commands
{
    public abstract class Command
    {
        public long AssignedIndex { get; set; }

        public TaskCompletionSource<object> Completion { get; set; }

        public object CommandResult { get; set; }

        public void Complete()
        {
            Completion?.SetResult(CommandResult);
        }
    }
}
