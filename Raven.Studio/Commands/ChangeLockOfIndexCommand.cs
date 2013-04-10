using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Silverlight.Connection;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
    public class ChangeLockOfIndexCommand : Command
    {
        private readonly string name;
        private readonly IndexLockMode lockMode;

        public ChangeLockOfIndexCommand(string name, IndexLockMode lockMode)
        {
            this.name = name;
            this.lockMode = lockMode;
        }

        public override void Execute(object parameter)
        {
            AskUser.ConfirmationAsync("Confirm Lock Change",
                                      string.Format(
                                          "Are you sure that you want to change the lock mode of this index? ({0})",
                                          name))
                   .ContinueWhenTrue(() => ChangeLock(name))
                   .Unwrap()
                   .Catch();
        }

        private Task ChangeLock(string index)
        {
            return ApplicationModel
                .DatabaseCommands
                .CreateRequest("/indexes/" + index + "?op=lockModeChange&mode=" + lockMode, "POST")
                .ExecuteRequestAsync();
        }
    }
}
