using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Threading
{
    public interface IDisposeOnceOperationMode {}
    public struct ExceptionRetry : IDisposeOnceOperationMode { }
    public struct SingleAttempt : IDisposeOnceOperationMode { }

    public class DisposeOnce<TOperationMode>
        where TOperationMode : struct, IDisposeOnceOperationMode
    {
        private readonly MultipleUseFlag _disposeInProgress = new MultipleUseFlag();
        private TaskCompletionSource<object> _disposeCompleted = new TaskCompletionSource<object>();
        private readonly Action _action;

        public DisposeOnce(Action action)
        {
            _action = action;
        }

        /// <summary>
        /// Runs the dispose action. Ensures any threads that are running it
        /// concurrently wait for the dispose to finish if it is in progress.
        /// 
        /// If the dispose has already happened, the <see cref="TOperationMode"/> defines
        /// how Dispose will react. The two approaches differ only in error
        /// handling.
        /// 
        /// When behavior is <see cref="ExceptionRetry"/>, we will retry the
        /// Dispose until it succeeds. Retry, however, happens on successive
        /// calls to Dispose, rather than in a single attempt.
        /// 
        /// When behavior is <see cref="SingleAttempt"/>, a failure means all
        /// subsequent calls will fail by throwing the same exception that
        /// was thrown by the action.
        /// </summary>
        public void Dispose()
        {
            if (_disposeInProgress.Raise() == false)
            {
                try
                {
                    // If a dispose is in progress, all other threads
                    // attempting to dispose will stop here and wait until it
                    // is over.
                    _disposeCompleted.Task.Wait();
                }
                catch (AggregateException e)
                {
                    // Theres two reasons we may be here, either we were 
                    // waiting and it failed, or we never got to wait because
                    // it was already errored. In either way, this exception
                    // is an AggregateException.
                    throw e.InnerException;
                }

                return;
            }

            try
            {
                _action();

                // Let everyone know this run worked out!
                _disposeCompleted.SetResult(null);
            }
            catch (Exception e)
            {                
                // Let everyone waiting know that this run failed
                _disposeCompleted.SetException(e);

                // Reset the state for the next attempt. Order of operations here is 
                // important: the new round starts when _disposeInProgress is set to
                // false. Since we are using GC, the threads waiting on the completion
                // won't end up with null references.
                if (typeof(TOperationMode) == typeof(ExceptionRetry))
                {
                    Interlocked.Exchange(ref _disposeCompleted, new TaskCompletionSource<object>());
                    _disposeInProgress.LowerOrDie();
                }
                else if (typeof(TOperationMode) != typeof(SingleAttempt))
                {
                    // This is here to prevent people from writing bad code. 
                    // It will fail to compile if the operation mode is not
                    // properly handled in the code.
                    var configurationGuard = new object[typeof(TOperationMode) != typeof(SingleAttempt) ? -1 : 0];
                    GC.KeepAlive(configurationGuard);
                }

                // Rethrow so that our thread knows it failed
                throw;
            }
        }
    }
}
