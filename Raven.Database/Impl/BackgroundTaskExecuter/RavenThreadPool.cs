using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Sparrow.Collections;

namespace Raven.Database.Impl.BackgroundTaskExecuter
{
    public class RavenThreadPool : IDisposable, ICpuUsageHandler
    {
        [ThreadStatic] private static Thread _selfInformation;

        private readonly ConcurrentQueue<CountdownEvent> _concurrentEvents = new ConcurrentQueue<CountdownEvent>();
        private readonly CancellationTokenSource _createLinkedTokenSource;
        private readonly CancellationToken _ct;
        private readonly object _locker = new object();
        private readonly ConcurrentDictionary<ThreadTask, object> _runningTasks = new ConcurrentDictionary<ThreadTask, object>();
        private readonly BlockingCollection<ThreadTask> _tasks = new BlockingCollection<ThreadTask>();
        private readonly AutoResetEvent _threadHasNoWorkToDo = new AutoResetEvent(true);
        private readonly ILog logger = LogManager.GetCurrentClassLogger();
        private int _currentWorkingThreadsAmount;
        public ThreadLocal<bool> _freedThreadsValue = new ThreadLocal<bool>(true);
        public int _partialMaxWait = 2500;
        private int _partialMaxWaitChangeFlag = 1;
        private int _hasPartialBatchResumption = 0;
        private Thread[] _threads;

        private const int defaultPageSize = 1024;

        public static int DefaultPageSize => defaultPageSize;

        public RavenThreadPool(int maxLevelOfParallelism)
        {
            _createLinkedTokenSource = new CancellationTokenSource();
            _ct = _createLinkedTokenSource.Token;
            _threads = new Thread[maxLevelOfParallelism];

            for (var i = 0; i < _threads.Length; i++)
            {
                var copy = i;
                _threads[i] = 
                    new Thread(() => ExecutePoolWork(_threads[copy]))
                    {
                        Name = "S " + copy,
                        IsBackground = true,
                        Priority = ThreadPriority.Normal
                    
                };
            }

            _currentWorkingThreadsAmount = maxLevelOfParallelism;
            CpuStatistics.RegisterCpuUsageHandler(this);
        }

        public Action<AutoTunerDecisionDescription> ReportToAutoTuner = x => { };

        public void HandleHighCpuUsage()
        {
            lock (_locker)
            {
                try
                {
                    if (_threads == null)
                        return;

                    // reducing TP threads priority
                    foreach (var thread in _threads)
                    {
                        if (thread.IsAlive == false)
                        {
                            logger.Warn("High CPU handle was called on a dead thread of RavenThreadPool");
                        }

                        if (thread.Priority != ThreadPriority.Normal)
                            continue;

                        ReportToAutoTuner(
                            new AutoTunerDecisionDescription(
                                name: "RavenThreadPool",
                                dbname: null,
                                reason: $"Reduced thread #{thread.ManagedThreadId} priority was changed to below normal priority {ThreadPriority.BelowNormal} because of high CPU Usage"
                            ));
                        thread.Priority = ThreadPriority.BelowNormal;
                        return;
                    }
                    
                }
                catch (Exception e)
                {
                    logger.ErrorException($"Error occured while throttling down RavenThreadPool", e);
                    throw;
                }
            }
        }

        public void HandleLowCpuUsage()
        {
            lock (_locker)
            {
                try
                {
                    if (_threads == null)
                        return;

                    // first step of handling low cpu is raising threads priority to normal
                    foreach (var thread in _threads)
                    {
                        if (thread.IsAlive == false)
                            return;
                        if (thread.Priority != ThreadPriority.BelowNormal)
                            continue;

                        var reason = $"Thread #{thread.ManagedThreadId} priority was changed to normal priority {ThreadPriority.Normal} because of low CPU Usage";
                        ReportToAutoTuner(new AutoTunerDecisionDescription("RavenThreadPool", null, reason));
                        thread.Priority = ThreadPriority.Normal;
                        return;
                    }
                }
                catch (Exception e)
                {
                    logger.ErrorException($"Error occured while throttling up RavenThreadPool", e);
                    throw;
                }
            }
        }

        public void Dispose()
        {
            lock (_locker)
            {
                if (_threads == null)
                    return;
                try
                {
                    DrainThePendingTasks();
                    _createLinkedTokenSource.Cancel();
                    var concurrentEvents = new List<CountdownEvent>();
                    CountdownEvent ce;
                    while (_concurrentEvents.TryDequeue(out ce))
                        concurrentEvents.Add(ce);

                    foreach (var concurrentEvent in concurrentEvents)
                    {
                        if (concurrentEvent.IsSet == false)
                            concurrentEvent.Signal();
                    }
                    foreach (var t in _threads)
                    {
                        t.Join();
                    }
                    foreach (var concurrentEvent in concurrentEvents)
                    {
                        concurrentEvent.Dispose();
                    }
                    _threads = null;
                }
                catch (Exception e)
                {
                    logger.ErrorException($"Error occured while disposing RavenThreadPool", e);
                    throw;
                }
            }
        }

        public ThreadsSummary GetThreadPoolStats()
        {
            var ts = new ThreadsSummary
            {
                ThreadsPrioritiesCounts = new ConcurrentDictionary<ThreadPriority, int>(),
                PartialMaxWait = Thread.VolatileRead(ref _partialMaxWait),
                FreeThreadsAmount = _freedThreadsValue.Values.Count(isFree => isFree),
                ConcurrentEventsCount = _concurrentEvents.Count,
                ConcurrentWorkingThreadsAmount = Thread.VolatileRead(ref _currentWorkingThreadsAmount)
            };
            Parallel.ForEach(_threads, thread =>
            {
                ts.ThreadsPrioritiesCounts.AddOrUpdate(thread.Priority, 1, (tp, val) => val + 1);
            });
            return ts;
        }

        public RavenThreadPool Start()
        {
            foreach (var t in _threads)
            {
                t.Start();
            }
            return this;
        }

        public void DrainThePendingTasks()
        {
            do
            {
                ExecuteWorkOnce(shouldWaitForWork: false);
            } while (_tasks.Count > 0);
        }

        public ThreadTask[] GetRunningTasks()
        {
            return _runningTasks.Keys.ToArray();
        }

        public ThreadTask[] GetAllWaitingTasks()
        {
            return _tasks.ToArray();
        }

        public object GetDebugInfo()
        {
            return new
            {
                ThreadPoolStats = GetThreadPoolStats(),
                WaitingTasks = GetAllWaitingTasks().Select(x => new {Database = x.Database?.Name, x.Description}),
                RunningTasks = GetRunningTasks().Select(x => new {Database = x.Database?.Name, x.Description})
                
            };
        }

        public int WaitingTasksAmount
        {
            get { return _tasks.Count; }
        }

        public int RunningTasksAmount
        {
            get { return _runningTasks.Count; }
        }

        public Action<Alert> ReportAlert = x => { };

        private void ExecutePoolWork(Thread selfData)
        {
            try
            {
                _freedThreadsValue.Value = true;
                while (_ct.IsCancellationRequested == false) // cancellation token
                {
                    _selfInformation = selfData;

                    ExecuteWorkOnce();

                    _selfInformation = null;
                    _freedThreadsValue.Value = true;
                }
            }
            catch (OperationCanceledException)
            {
                //expected
            }
            catch (Exception e)
            {
                const string error = "Error while running background tasks, this is very bad";
                logger.FatalException(error, e);
                ReportAlert(new Alert
                {
                    AlertLevel = AlertLevel.Error,
                    CreatedAt = DateTime.UtcNow,
                    Title = error,
                    UniqueKey = error,
                    Message = e.ToString(),
                    Exception = e.Message
                });
            }
        }

        private void ExecuteWorkOnce(bool shouldWaitForWork = true)
        {
            ThreadTask threadTask = null;
            if (shouldWaitForWork == false && _tasks.TryTake(out threadTask) == false)
            {
                return;
            }

            if (threadTask == null)
                threadTask = GetNextTask();

            if (threadTask == null)
                return;

            if (Interlocked.CompareExchange(ref threadTask.Proccessing, 1, 0) == 1)
                return;

            RunThreadTask(threadTask);
            
        }

        private void RunThreadTask(ThreadTask threadTask)
        {
            try
            {
                threadTask.Duration = Stopwatch.StartNew();
              
                try
                {
                    if (threadTask.Database?.Disposed == true)
                    {
                        logger.Warn($"Ignoring request to run threadTask because the database ({threadTask.Database.Name}) is been disposed.");
                        return;
                    }
                    _runningTasks.TryAdd(threadTask, null);
                    threadTask.Action();
                }
                finally
                {
                    if (threadTask.BatchStats != null)
                    {
                        var itemsCompleted = Interlocked.Increment(ref threadTask.BatchStats.Completed);
                        if (itemsCompleted == threadTask.BatchStats.Total)
                        {
                            threadTask.RunAfterCompletion?.Invoke();
                        }
                    }
                        
                    threadTask.Duration.Stop();
                    object _;
                    _runningTasks.TryRemove(threadTask, out _);
                }
            }
            catch (Exception e)
            {
                logger.ErrorException(
                    $"Error occured while executing RavenThreadPool task ; Database name: {threadTask?.Database?.Name} ; " +
                    $"Task queued at: {threadTask?.QueuedAt} ; Task Description: {threadTask?.Description}", e);
            }
        }

        private ThreadTask GetNextTask()
        {
            ThreadTask threadTask = null;

            var timeout = 1000;
            while (!_ct.IsCancellationRequested)
            {
                if (_tasks.TryTake(out threadTask, timeout, _ct))
                    break;

                _threadHasNoWorkToDo.Set();
                if (Thread.VolatileRead(ref _hasPartialBatchResumption) == 0)
                {
                    timeout = 5*60*1000;
                }
                else
                {
                    timeout = 2*1000;
                }
            }
            _freedThreadsValue.Value = false;
            return threadTask;
        }

        /// <summary>
        /// Executes given function in batches on received collection
        /// </summary>
        public void ExecuteBatch<T>(IList<T> src, Action<IEnumerator<T>> action, DocumentDatabase database = null,
            int pageSize = defaultPageSize, string description = null)
        {
            if (src.Count == 0)
                return;

            if (src.Count <= pageSize)
            {
                //if we have only none or less than pageSize,
                //we should execute it in the current thread, without using RTP threads
                ExecuteSingleBatchSynchronously(src, action, description, database);
                return;
            }

            var ranges = new ConcurrentQueue<Tuple<int, int>>();
            var now = DateTime.UtcNow;
            var numOfBatches = (src.Count/pageSize) + (src.Count%pageSize == 0 ? 0 : 1);

            CountdownEvent batchesCountdown;
            if (_concurrentEvents.TryDequeue(out batchesCountdown) == false)
                batchesCountdown = new CountdownEvent(numOfBatches);
            else
                batchesCountdown.Reset(numOfBatches);

            var localTasks = new List<ThreadTask>();
            var exceptions = new ConcurrentSet<Exception>();

            for (var i = 0; i < src.Count; i += pageSize)
            {
                var rangeStart = i;
                var rangeEnd = i + pageSize - 1 < src.Count ? i + pageSize - 1 : src.Count - 1;
                ranges.Enqueue(Tuple.Create(rangeStart, rangeEnd));

                var threadTask = new ThreadTask
                {
                    Action = () =>
                    {
                        var numOfBatchesUsed = new Reference<int>();
                        try
                        {
                            database?.WorkContext.CancellationToken.ThrowIfCancellationRequested();
                            Tuple<int, int> range;
                            if (ranges.TryDequeue(out range) == false)
                                return;

                            action(YieldFromRange(ranges, range, src, numOfBatchesUsed, database?.WorkContext.CancellationToken));
                        }
                        catch (Exception e)
                        {
                            exceptions.Add(e);
                            throw;
                        }
                        finally
                        {
                            // we will try to release the countdown event if we got here because cancellation token was requested, else wi will signal the countdown event according to the number of batches we've proccessed
                            if (database?.WorkContext.CancellationToken.IsCancellationRequested??false)
                            {
                                try
                                {
                                    while (batchesCountdown.CurrentCount!= 0 )
                                        batchesCountdown.Signal(1);
                                }
                                catch 
                                {
                                    
                                }
                            }
                            else
                            {
                                var numOfBatchesUsedValue = numOfBatchesUsed.Value;
                                //handle the case when we are out of ranges
                                if (numOfBatchesUsedValue > 0)
                                {
                                    try
                                    {
                                        batchesCountdown.Signal(numOfBatchesUsedValue);
                                    }
                                    catch (Exception)
                                    {
                                    }
                                }
                            }
                        }
                    },
                    Description = new OperationDescription
                    {
                        Type = OperationDescription.OperationType.Range,
                        PlainText = description,
                        From = rangeStart,
                        To = rangeEnd,
                        Total = src.Count
                    },
                    Database = database,
                    BatchStats = new BatchStatistics
                    {
                        Total = rangeEnd - rangeStart,
                        Completed = 0
                    },
                    QueuedAt = now,
                    DoneEvent = batchesCountdown,
                };
                localTasks.Add(threadTask);
            }

            // we must add the tasks to the global tasks after we added all the ranges
            // to prevent the tasks from completing the range fast enough that it won't
            // see the next range, see: http://issues.hibernatingrhinos.com/issue/RavenDB-4829
            foreach (var threadTask in localTasks)
            {
                _tasks.Add(threadTask, _ct);
            }

            WaitForBatchToCompletion(batchesCountdown, localTasks, database?.WorkContext.CancellationToken);

            switch (exceptions.Count)
            {
                case 0:
                    return;
                case 1:
                    ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
                    break;
                default:
                    throw new AggregateException(exceptions);
            }
        }

        private void ExecuteSingleBatchSynchronously<T>(IList<T> src, Action<IEnumerator<T>> action, string description, DocumentDatabase database)
        {
            using (var enumerator = src.GetEnumerator())
            {
                var threadTask = new ThreadTask
                {
                    Action = () => { action(enumerator); },
                    BatchStats = new BatchStatistics
                    {
                        Total = src.Count,
                        Completed = 0
                    },
                    EarlyBreak = false,
                    QueuedAt = DateTime.UtcNow,
                    Description = new OperationDescription
                    {
                        From = 1,
                        To = src.Count,
                        Total = src.Count,
                        PlainText = description
                    },
                    Database = database
                };

                try
                {
                    _runningTasks.TryAdd(threadTask, null);
                    if (database?.Disposed == true)
                    {
                        logger.Warn($"Ignoring request to run a single batch because the database ({database.Name}) is been disposed.");
                        return;
                    }
                    threadTask.Action();
                    threadTask.BatchStats.Completed++;
                }
                catch (Exception e)
                {
                    logger.Error(
                        $"Error occured while executing RavenThreadPool task ; Database name: {threadTask.Database?.Name} ;" +
                        $"Task queued at: {threadTask.QueuedAt} ; Task Description: {threadTask.Description}", e);
                    
                    throw;
                }
                finally
                {
                    object _;
                    _runningTasks.TryRemove(threadTask, out _);
                }
            }
        }

        private IEnumerator<T> YieldFromRange<T>(ConcurrentQueue<Tuple<int, int>> ranges, Tuple<int, int> boundaries, IList<T> input, Reference<int> numOfBatchesUsed, CancellationToken? ct)
        {
            var cancellationToken = ct ?? _ct;
            do
            {
                numOfBatchesUsed.Value++;
                for (var i = boundaries.Item1; i <= boundaries.Item2 && cancellationToken.IsCancellationRequested == false; i++)
                {
                    yield return input[i];
                }
            } while (ranges.TryDequeue(out boundaries) && cancellationToken.IsCancellationRequested == false);

        }

        public bool ExecuteBatch<T>(IList<T> src, Action<T> action, DocumentDatabase database = null, string description = null,
            bool allowPartialBatchResumption = false, Action runAfterCompletion = null)
        {
            //, int completedMultiplier = 2, int freeThreadsMultiplier = 2, int maxWaitMultiplier = 1
            switch (src.Count)
            {
                case 0:
                    return true;
                case 1:
                    //if we have only one source to go through,
                    //we should execute it in the current thread, without using RTP threads
                    ExecuteSingleBatchSynchronously(src, action, description, database);
                    runAfterCompletion?.Invoke();
                    return true;
            }

            var now = DateTime.UtcNow;
            CountdownEvent countdownEvent;
            var itemsCount = 0;

            var batch = new BatchStatistics
            {
                Total = src.Count,
                Completed = 0
            };

            if (_concurrentEvents.TryDequeue(out countdownEvent) == false)
                countdownEvent = new CountdownEvent(src.Count);
            else
                countdownEvent.Reset(src.Count);

            var exceptions = new ConcurrentSet<Exception>();
            var currentBatchTasks = new List<ThreadTask>(src.Count);
            for (; itemsCount < src.Count && _ct.IsCancellationRequested == false; itemsCount++)
            {
                var copy = itemsCount;
                var threadTask = new ThreadTask
                {
                    Action = () =>
                    {
                        try
                        {
                            database?.WorkContext.CancellationToken.ThrowIfCancellationRequested();
                            action(src[copy]);
                        }
                        catch (Exception e)
                        {
                            exceptions.Add(e);
                            throw;
                        }
                        finally
                        {
                            countdownEvent.Signal();
                        }
                    },
                    Description = new OperationDescription
                    {
                        Type = OperationDescription.OperationType.Atomic,
                        PlainText = description,
                        From = itemsCount + 1,
                        To = itemsCount + 1,
                        Total = src.Count
                    },
                    Database = database,
                    BatchStats = batch,
                    EarlyBreak = allowPartialBatchResumption,
                    QueuedAt = now,
                    DoneEvent = countdownEvent,
                    Proccessing = 0,
                    RunAfterCompletion = runAfterCompletion
                };
                _tasks.Add(threadTask);
                currentBatchTasks.Add(threadTask);
            }

            var ranToCompletion = false;
            if (allowPartialBatchResumption == false)
            {
                WaitForBatchToCompletion(countdownEvent, currentBatchTasks, database?.WorkContext.CancellationToken);
                ranToCompletion = true;
            }
            else
            {
                ranToCompletion = WaitForBatchAllowingPartialBatchResumption(countdownEvent, batch,currentBatchTasks, database);
            }

            switch (exceptions.Count)
            {
                case 0:
                    return ranToCompletion;
                case 1:
                    ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
                    return ranToCompletion; // won't happen
                default:
                    throw new AggregateException(exceptions);
            }

            
        }

        private void ExecuteSingleBatchSynchronously<T>(IList<T> src, Action<T> action, string description, DocumentDatabase database)
        {
            var threadTask = new ThreadTask
            {
                Action = () => { action(src[0]); },
                BatchStats = new BatchStatistics
                {
                    Total = 1,
                    Completed = 0
                },
                EarlyBreak = false,
                QueuedAt = DateTime.UtcNow,
                Description = new OperationDescription
                {
                    From = 1,
                    To = 1,
                    Total = 1,
                    PlainText = description
                },
                Database= database
            };

            try
            {
                _runningTasks.TryAdd(threadTask, null);
                if (database?.Disposed == true)
                {
                    logger.Warn($"Ignoring request to run a single batch because the database ({database.Name}) is been disposed.");
                    return;
                }
                threadTask.Action();
                threadTask.BatchStats.Completed++;
            }
            catch (Exception e)
            {
                if (logger.IsDebugEnabled)
                {
                    logger.DebugException(
                     "Error occured while executing RavenThreadPool task ; " +
                     $"Database name: {threadTask.Database?.Name} ; Task queued at: {threadTask.QueuedAt} ; " +
                     $"Task Description: {threadTask.Description}", e);
                }

                throw;
            }
            finally
            {
                object _;
                _runningTasks.TryRemove(threadTask, out _);
            }
        }

        /// <summary>
        /// Performs busy wait, helping running child tasks
        /// </summary>
        /// <param name="completionEvent"></param>
        /// <param name="childTasks"></param>
        /// <param name="workContextCancellationToken"></param>
        private void WaitForBatchToCompletion(CountdownEvent completionEvent, List<ThreadTask> childTasks, CancellationToken? workContextCancellationToken = null)
        {
            try
            {
                for (int index = 0; index < childTasks.Count && completionEvent.IsSet == false && _ct.IsCancellationRequested == false; index++)
                {
                    var task = childTasks[index];
                    if (Interlocked.CompareExchange(ref task.Proccessing, 1, 0) == 1)
                        continue;
                    RunThreadTask(task);
                }

                // maybe in the future, add busy wait for children's children etc.
                completionEvent.Wait(workContextCancellationToken??_ct);
                
            }
            finally
            {
                _concurrentEvents.Enqueue(completionEvent);
            }
        }

        private bool WaitForBatchAllowingPartialBatchResumption(CountdownEvent completionEvent, BatchStatistics batch, List<ThreadTask> childTasks, DocumentDatabase database = null)
        {
            Interlocked.Increment(ref _hasPartialBatchResumption);
            var cancellationToken = database?.WorkContext.CancellationToken ?? _ct;
            try
            {
                var waitHandles = new[] { completionEvent.WaitHandle, _threadHasNoWorkToDo };
                var sp = Stopwatch.StartNew();
                var batchRanToCompletion = false;
                var lastThreadIndexChecked = 0;
                while (cancellationToken.IsCancellationRequested == false)
                {
                    // First, try to find work to do among child tasks, instead of just waiting
                    ThreadTask busyWaitWorkToDo = null;
                    for (; lastThreadIndexChecked  < childTasks.Count && completionEvent.IsSet == false && _ct.IsCancellationRequested == false; lastThreadIndexChecked++)
                    {
                        var task = childTasks[lastThreadIndexChecked];
                        if (Interlocked.CompareExchange(ref task.Proccessing, 1, 0) == 1)
                            continue;
                        busyWaitWorkToDo = task;
                        break;
                    }

                    int returnedWaitHandleIndex;

                    // Run found work or just wait for all work to finish (returnedWaitHandleIndex=0) or to be notified about free threads in the system (returnedWaitHandleIndex=1)
                    // After which we'll decide whether we should early exit, or to wait some more
                    if (busyWaitWorkToDo != null)
                    {
                        RunThreadTask(busyWaitWorkToDo);
                        returnedWaitHandleIndex = WaitHandle.WaitAny(waitHandles, 0);
                    }
                    else
                    {
                        returnedWaitHandleIndex = WaitHandle.WaitAny(waitHandles);
                    }
                    
                    if (returnedWaitHandleIndex == 0)
                        break;

                    // we won't consider breaking early if we haven't completed at least half the work
                    if (Thread.VolatileRead(ref batch.Completed) < batch.Total / 2)
                        continue;

                    var currentFreeThreads = _freedThreadsValue.Values.Count(isFree => isFree);

                    // we will break early only if there are more then half free threads
                    if (currentFreeThreads > _currentWorkingThreadsAmount / 2)
                    {
                        break;
                    }
                }

                // after we've decided to quit early, we will wait some more time, allowing a normal wait. 
                // we decide how much we will wait by choosing the biggest among the next: 
                // 1) half the time we've waited on the current batch 
                // 2) a waiting time factor that increase for every second left early batch and decreases for every leave early batch that completed before leaving early

                var elapsedMilliseconds = (int)sp.ElapsedMilliseconds;
                if (completionEvent.Wait(Math.Max(elapsedMilliseconds / 2, Thread.VolatileRead(ref _partialMaxWait)) , cancellationToken))
                {
                    _concurrentEvents.Enqueue(completionEvent);
                }

                // updating the waiting time factor
                if (batch.Completed != batch.Total)
                {
                    if (_partialMaxWaitChangeFlag > 0)
                    {
                        Interlocked.Exchange(ref _partialMaxWait, Math.Min(2500, (int)(Thread.VolatileRead(ref _partialMaxWait) * 1.25)));
                    }
                }
                else if (_partialMaxWaitChangeFlag < 0)
                {
                    batchRanToCompletion = true;
                    Interlocked.Exchange(ref _partialMaxWait, Math.Max(Thread.VolatileRead(ref _partialMaxWait) / 2, 10));
                }

                Interlocked.Exchange(ref _partialMaxWaitChangeFlag, Thread.VolatileRead(ref _partialMaxWaitChangeFlag) * -1);

                // completionEvent is explicitly left to the finalizer
                // we expect this to be rare, and it isn't worth the trouble of trying
                // to manage this

                if (logger.IsDebugEnabled)
                {
                    logger.Debug($"Raven Thread Pool ended batch for database {database?.Name}. Done {batch.Completed} items out of {batch.Total}. Batch {(batchRanToCompletion ? "was not" : "was")} left early");
                }

                return batchRanToCompletion;
            }
            finally
            {
                Interlocked.Decrement(ref _hasPartialBatchResumption);
            }
        }

        

        public class ThreadTask
        {
            public Action Action;
            public BatchStatistics BatchStats;
            public object Description;
            public CountdownEvent DoneEvent;
            public Stopwatch Duration;
            public bool EarlyBreak;
            public DateTime QueuedAt;
            public DocumentDatabase Database;
            public long Proccessing;
            public Action RunAfterCompletion { get; set; }
        }

        public class ThreadsSummary
        {
            public ConcurrentDictionary<ThreadPriority, int> ThreadsPrioritiesCounts { get; set; }
            public int PartialMaxWait { get; set; }
            public int FreeThreadsAmount { get; set; }
            public int ConcurrentEventsCount { get; set; }
            public int ConcurrentWorkingThreadsAmount { get; set; }
        }

        public class BatchStatistics
        {
            public enum BatchType
            {
                Atomic,
                Range
            }

            public int Completed;
            public int Total;
            public BatchType Type;
        }

        public class OperationDescription
        {
            public enum OperationType
            {
                Atomic,
                Range,
                Maintenance
            }

            public int From;
            public string PlainText;
            public int To;
            public int Total;
            public OperationType Type;

            public override string ToString()
            {
                return $"{PlainText} range {From} to {To} of {Total}";
            }
        }
    }
}
