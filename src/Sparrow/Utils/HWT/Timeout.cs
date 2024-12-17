
namespace Sparrow.Utils.HWT
{
    /// <summary>
    /// A handle associated with a TimerTask that is returned by a Timer
    /// </summary>
    internal interface Timeout
    {
        /// <summary>
        ///  Returns the Timer that created this handle.
        /// </summary>
        Timer Timer { get; }

        /// <summary>
        /// Returns the TimerTask which is associated with this handle.
        /// </summary>
        TimerTask TimerTask { get; }

        /// <summary>
        /// Returns true if and only if the TimerTask associated
        /// with this handle has been expired
        /// </summary>
        bool Expired { get; }

        /// <summary>
        /// Returns true if and only if the TimerTask associated
        /// with this handle has been cancelled
        /// </summary>
        bool Cancelled { get; }

        /// <summary>
        /// Attempts to cancel the {@link TimerTask} associated with this handle.
        /// If the task has been executed or cancelled already, it will return with no side effect.
        /// </summary>
        /// <returns>True if the cancellation completed successfully, otherwise false</returns>
        bool Cancel();
    }
}
