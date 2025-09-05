using System;
using System.Diagnostics;
using System.Threading;

namespace Raven.Server.Documents.Indexes;

public class LastQueriedTimeTracker
{
    /// <summary>
    /// Approximate time in ticks since the last query was executed.
    /// </summary>
    private long _elapsedSinceQueried;

    /// <summary>
    /// ElapsedSinceQueriedTicks update date in ticks.
    /// </summary>
    private long _lastElapsedUpdate;

    /// <summary>
    /// Actually the last query date in ticks. Initially it is set to databaseDate - elapsed.
    /// </summary>
    private long _lastQueriedDate;

    public TimeSpan ElapsedSinceQueried => new(Interlocked.Read(ref _elapsedSinceQueried));
    public DateTime LastQueryDate => new(Interlocked.Read(ref _lastQueriedDate), DateTimeKind.Utc);

    public LastQueriedTimeTracker(DateTime now, long elapsedSinceQueried)
    {
        Debug.Assert(now.Kind == DateTimeKind.Utc);
        _elapsedSinceQueried = elapsedSinceQueried;
        _lastElapsedUpdate = now.Ticks;
        _lastQueriedDate = now.Ticks - elapsedSinceQueried;
    }

    /// <summary>
    /// Reset the elapsed time and set the proper last query date.
    /// </summary>
    public void MarkQueried(in DateTime now)
    {
        Debug.Assert(now.Kind == DateTimeKind.Utc);

        var currentDate = Interlocked.Read(ref _lastElapsedUpdate);
        var currentElapsed = Interlocked.Read(ref _elapsedSinceQueried);
        
        // We already updated the date (to newer) in a different thread, and it was done in this method. Nothing to do.
        // In case when the current elapsed is not 0, it means we lost a race with UpdateElapsedFromLastQueried, and we need to clear the elapsed value.
        if (now.Ticks < currentDate && currentElapsed == 0)
            return;

        var previousUpdateDate = Interlocked.CompareExchange(ref _lastElapsedUpdate, now.Ticks, currentDate);

        if (previousUpdateDate != currentDate)
        {
            // We had an update in the meantime.
            // We need to clear _elapsedSinceQueriedTicks in case when the actual update happened in the indexing thread and increased the elapsed time.
            if (previousUpdateDate < now.Ticks && currentElapsed > 0)
            {
                Interlocked.CompareExchange(ref _lastElapsedUpdate, now.Ticks, previousUpdateDate);
            }
        }

        Interlocked.Exchange(ref _lastQueriedDate, now.Ticks);
        Interlocked.Exchange(ref _elapsedSinceQueried, 0);
    }

    /// <summary>
    /// This method is called from an indexing thread to increment the elapsed time
    /// </summary>
    /// <param name="now">Current date of an update.</param>
    /// <returns>Elapsed from the last query.</returns>
    public TimeSpan UpdateElapsedSinceQueried(DateTime now)
    {
        Debug.Assert(now.Kind == DateTimeKind.Utc);
        var currentUpdateDate = Interlocked.Read(ref _lastElapsedUpdate);

        if (currentUpdateDate > now.Ticks)
        {
            // Currently stored values are in the future from the current caller perspective.
            // This means that we need to persist the current value from the memory.
            // Since the only method called in the meantime can be MarkAsQueried,
            // we can safely return the current value since there was a new query.
            var elapsedSinceQueriedTicks = Interlocked.Read(ref _elapsedSinceQueried);
            Debug.Assert(elapsedSinceQueriedTicks == 0);
            
            return new(elapsedSinceQueriedTicks);
        }

        // The difference between now and the last update date in ticks.
        var elapsedDelta = now.Ticks - currentUpdateDate;
        var previousUpdateDate = Interlocked.CompareExchange(ref _lastElapsedUpdate, now.Ticks, currentUpdateDate);

        if (previousUpdateDate != currentUpdateDate)
        {
            // Another thread updated the value in the meantime.
            // Since this method is called only from indexing loop thread,
            // it means we had a query in the meantime so _elapsedSinceQueriedTicks is already updated.
            return new(Interlocked.Read(ref _elapsedSinceQueried));
        }

        return new(Interlocked.Add(ref _elapsedSinceQueried, elapsedDelta));
    }
}
