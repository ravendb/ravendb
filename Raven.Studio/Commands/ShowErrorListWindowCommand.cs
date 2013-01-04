using Raven.Studio.Features.Util;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;

namespace Raven.Studio.Commands
{
    public class ShowErrorListWindowCommand : Command
    {
        public override void Execute(object parameter)
        {
            ErrorListWindow.ShowErrors(parameter as Notification);
        }
    }
}