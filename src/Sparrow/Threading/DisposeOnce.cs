using System;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Threading
{
    public interface IDisposeOnceOperationMode
    {
        void Initialize();
        bool DuringDispose { get; }
        void EnterDispose();
        void LeaveDispose();

    }
    public struct ExceptionRetry : IDisposeOnceOperationMode {
        public void Initialize()
        {            
        }
        public bool DuringDispose => false;
        public void EnterDispose(){}
        public void LeaveDispose(){}
    }
    public struct SingleAttempt : IDisposeOnceOperationMode {
        private int disposeDepth;
        public void Initialize()
        {
            disposeDepth = 0;
        }
        public bool DuringDispose => disposeDepth != 0;
        public void EnterDispose()
        {
            disposeDepth++;
        }
        public void LeaveDispose()
        {
            disposeDepth--;
        }
    }

    public sealed class DisposeOnce<TOperationMode>
        where TOperationMode : struct, IDisposeOnceOperationMode
    {
        private readonly Action _action;
        private Tuple<MultipleUseFlag, TaskCompletionSource<object>> _state 
            = Tuple.Create(new MultipleUseFlag(), new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));

        TOperationMode _operationModeData;
             
        public DisposeOnce(Action action)
        {
            _action = action;
            if (typeof(TOperationMode) != typeof(ExceptionRetry) &&
                typeof(TOperationMode) != typeof(SingleAttempt))                
            {
                throw new NotSupportedException("Unknown operation mode: " + typeof(TOperationMode));
            }

            _operationModeData = default;
            _operationModeData.Initialize();
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
        /// When behavior is <see cref="SingleAttempt"/> or <see cref="SingleAttemptWithWaitForDisposeToFinish"/>, a failure means all
        /// subsequent calls will fail by throwing the same exception that
        /// was thrown by the action.
        /// </summary>
        public void Dispose()
        {
            _operationModeData.EnterDispose();
            try
            {
                var localState = _state;
                var disposeInProgress = localState.Item1;
                if (disposeInProgress.Raise() == false)
                {
                    // If a dispose is in progress, all other threads
                    // attempting to dispose will stop here and wait until it
                    // is over. This call to Wait may throw with an
                    // AggregateException
                    localState.Item2.Task.Wait();
                    return;
                }

                try
                {
                    _action();

                    // Let everyone know this run worked out!
                    localState.Item2.SetResult(null);
                }
                catch (Exception e)
                {
                    if (typeof(TOperationMode) == typeof(ExceptionRetry))
                    {
                        // Reset the state for the next attempt. First backup the
                        // current task completion.
                        // Let everyone waiting know that this run failed
                        localState.Item2.SetException(e);

                        // atomically replace both the flag and the task to wait, so new 
                        // callers to the Dispose are either getting the error or can start
                        // calling this again
                        Interlocked.CompareExchange(ref _state,
                            Tuple.Create(new MultipleUseFlag(), new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously)),
                            localState
                        );

                    }
                    else if (typeof(TOperationMode) == typeof(SingleAttempt))
                    {
                        // Let everyone waiting know that this run failed
                        localState.Item2.SetException(e);
                    }
                    else
                    {
                        throw new NotSupportedException("Unknown operation mode: " + typeof(TOperationMode));
                    }

                    throw;
                }
            }
            finally
            {
                _operationModeData.LeaveDispose();
            }
        }

        public bool Disposed
        {
            get
            {
                var state = _state;
                if (state.Item1 == false)
                    return false;

                if (typeof(TOperationMode) == typeof(SingleAttempt) && _operationModeData.DuringDispose == false)
                    return true;

                if (typeof(TOperationMode) == typeof(ExceptionRetry) || typeof(TOperationMode) == typeof(SingleAttempt))
                {
                    if (state.Item2.Task.IsFaulted || state.Item2.Task.IsCanceled)
                        return false;

                    return state.Item2.Task.IsCompleted;
                }


                throw new NotSupportedException("Unknown operation mode: " + typeof(TOperationMode));
            }
        }
    }
    
    
    public sealed class DisposeOnceAsync<TOperationMode>
        where TOperationMode : struct, IDisposeOnceOperationMode
    {
        private readonly Func<Task> _action;
        private Tuple<MultipleUseFlag, TaskCompletionSource<object>> _state 
            = Tuple.Create(new MultipleUseFlag(), new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));
        TOperationMode _operationModeData;

        public DisposeOnceAsync(Func<Task> action)
        {
            _action = action;
            if (typeof(TOperationMode) != typeof(ExceptionRetry) &&
                typeof(TOperationMode) != typeof(SingleAttempt)) 
            {
                throw new NotSupportedException("Unknown operation mode: " + typeof(TOperationMode));
            }
            _operationModeData = default;
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
        /// When behavior is <see cref="SingleAttempt"/> or <see cref="SingleAttemptWithWaitForDisposeToFinish"/>, a failure means all
        /// subsequent calls will fail by throwing the same exception that
        /// was thrown by the action.
        /// </summary>
        public async Task DisposeAsync()
        {
            _operationModeData.EnterDispose();

            try
            {
                var localState = _state;
                var disposeInProgress = localState.Item1;
                if (disposeInProgress.Raise() == false)
                {
                    // If a dispose is in progress, all other threads
                    // attempting to dispose will stop here and wait until it
                    // is over. This call to Wait may throw with an
                    // AggregateException
                    await localState.Item2.Task.ConfigureAwait(false);
                }

                try
                {
                    await _action().ConfigureAwait(false);

                    // Let everyone know this run worked out!
                    localState.Item2.SetResult(null);
                }
                catch (Exception e)
                {
                    if (typeof(TOperationMode) == typeof(ExceptionRetry))
                    {
                        // Reset the state for the next attempt. First backup the
                        // current task completion.
                        // Let everyone waiting know that this run failed
                        localState.Item2.SetException(e);

                        // atomically replace both the flag and the task to wait, so new 
                        // callers to the Dispose are either getting the error or can start
                        // calling this again
                        Interlocked.CompareExchange(ref _state,
                            Tuple.Create(new MultipleUseFlag(), new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously)),
                            localState
                        );

                    }
                    else if (typeof(TOperationMode) == typeof(SingleAttempt))
                    {
                        // Let everyone waiting know that this run failed
                        localState.Item2.SetException(e);
                    }
                    else
                    {
                        throw new NotSupportedException("Unknown operation mode: " + typeof(TOperationMode));
                    }

                    // Rethrow so that our thread knows it failed
                    throw new AggregateException(e);
                }
            }
            finally
            {
                _operationModeData.LeaveDispose();
            }
        }

        public bool Disposed
        {
            get
            {
                var state = _state;
                if (state.Item1 == false)
                    return false;

                if (typeof(TOperationMode) == typeof(SingleAttempt) && _operationModeData.DuringDispose == false)
                    return true;

                if (typeof(TOperationMode) == typeof(ExceptionRetry) || typeof(TOperationMode) == typeof(SingleAttempt))
                {
                    if (state.Item2.Task.IsFaulted || state.Item2.Task.IsCanceled)
                        return false;

                    return state.Item2.Task.IsCompleted;
                }


                throw new NotSupportedException("Unknown operation mode: " + typeof(TOperationMode));
            }
        }
    }
}
