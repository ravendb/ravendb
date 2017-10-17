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

    public sealed class DisposeOnce<TOperationMode>
        where TOperationMode : struct, IDisposeOnceOperationMode
    {
        private readonly Action _action;
        private readonly MultipleUseFlag _disposeInProgress = new MultipleUseFlag();
        private TaskCompletionSource<object> _disposeCompleted = new TaskCompletionSource<object>();

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
                // If a dispose is in progress, all other threads
                // attempting to dispose will stop here and wait until it
                // is over. This call to Wait may throw with an
                // AggregateException
                _disposeCompleted.Task.Wait();
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
                if (typeof(TOperationMode) == typeof(ExceptionRetry))
                {
                    // Reset the state for the next attempt. First backup the
                    // current task completion.
                    var oldDisposeCompleted = _disposeCompleted;

                    // Reset the TSC. All new threads entering the Dispose
                    // from this point and on will have to wait until the flag
                    // is set to low and another thread enters the Dispose.
                    Interlocked.Exchange(ref _disposeCompleted, new TaskCompletionSource<object>());

                    // Let everyone waiting know that this run failed
                    oldDisposeCompleted.SetException(e);

                    // The new round starts when _disposeInProgress is lowered.
                    // Since we are using GC, the threads waiting on the
                    // completion won't end up with null references.
                    _disposeInProgress.LowerOrDie();

                    // NOTICE: There is an interim moment in which the threads
                    // that enter Dispose will wait on a new TSC those Task is
                    // not actually in progress. They will wait on it until a
                    // different thread goes into the Dispose and effectively
                    // runs the action and sets them free.
                }
                else if (typeof(TOperationMode) == typeof(SingleAttempt))
                {
                    // Let everyone waiting know that this run failed
                    _disposeCompleted.SetException(e);
                }
                else
                {
                    // This is here to prevent people from writing bad code. 
                    // It will fail to compile if the operation mode is not
                    // properly handled in the code.
                    var configurationGuard = new object[typeof(TOperationMode) != typeof(SingleAttempt) ? -1 : 0];
                    GC.KeepAlive(configurationGuard);
                }

                // Rethrow so that our thread knows it failed
                throw new AggregateException(e);
            }
        }
    }
}
