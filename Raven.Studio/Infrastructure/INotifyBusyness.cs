using System;

namespace Raven.Studio.Infrastructure
{
    public interface INotifyBusyness
    {
        event EventHandler<EventArgs> IsBusyChanged;
        bool IsBusy { get; }
    }
}