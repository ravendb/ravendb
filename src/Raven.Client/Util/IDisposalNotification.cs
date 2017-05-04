using System;

namespace Raven.Client.Util
{
    /// <summary>
    /// Provide a way for interested party to tell whether implementers have been disposed
    /// </summary>
    public interface IDisposalNotification : IDisposable
    {
        /// <summary>
        /// Called after dispose is completed
        /// </summary>
        event EventHandler AfterDispose;

        /// <summary>
        /// Whether the instance has been disposed
        /// </summary>
        bool WasDisposed { get; }
    }
}
