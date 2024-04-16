using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client;

public class AsyncHelperTest : RavenTestBase
{
    public AsyncHelperTest(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void RunSync_WhenThrow_ShouldThrowSameExceptionWithAndWithoutAsyncContest()
    {
        var oldContext = SynchronizationContext.Current;
        TestException testException = new TestException();
        try
        {
            SynchronizationContext.SetSynchronizationContext(null);
            var withoutContext = Assert.ThrowsAny<Exception>(() => AsyncHelpers.RunSync(() => throw testException));

            SynchronizationContext.SetSynchronizationContext(new SimpleSynchronizationContext());
            var withContext = Assert.ThrowsAny<Exception>(() => AsyncHelpers.RunSync(() => throw testException));
                
            Assert.Equal(withoutContext.GetType(), withContext.GetType());
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(oldContext);
        }
    }
        
    [RavenFact(RavenTestCategory.ClientApi)]
    public void RunSyncVoid_WhenThrowAggregateException_ShouldThrowSameExceptionWithAndWithoutAsyncContest()
    {
        var oldContext = SynchronizationContext.Current;

        var aggregateException = new AggregateException(new List<Exception>{new TestException()});
        try
        {
            SynchronizationContext.SetSynchronizationContext(null);
            var withoutContext = Assert.ThrowsAny<Exception>(() => AsyncHelpers.RunSync(ThrowAggregateExceptionAsync));

            SynchronizationContext.SetSynchronizationContext(new SimpleSynchronizationContext());
            var withContext = Assert.ThrowsAny<Exception>(() => AsyncHelpers.RunSync(ThrowAggregateExceptionAsync));
        
            Assert.Equal(withoutContext.GetType(), withContext.GetType());
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(oldContext);
        }

        return;
        Task ThrowAggregateExceptionAsync() => Task.FromException(aggregateException);
    }
        
    [RavenFact(RavenTestCategory.ClientApi)]
    public void RunSyncReturnWithTask_WhenThrowAggregateException_ShouldThrowSameExceptionWithAndWithoutAsyncContest()
    {
        var oldContext = SynchronizationContext.Current;

        var aggregateException = new AggregateException(new List<Exception>{new TestException()});
        try
        {
            SynchronizationContext.SetSynchronizationContext(null);
            var withoutContext = Assert.ThrowsAny<Exception>(() => AsyncHelpers.RunSync(ThrowAggregateExceptionAsync));

            SynchronizationContext.SetSynchronizationContext(new SimpleSynchronizationContext());
            var withContext = Assert.ThrowsAny<Exception>(() => AsyncHelpers.RunSync(ThrowAggregateExceptionAsync));
        
            Assert.Equal(withoutContext.GetType(), withContext.GetType());
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(oldContext);
        }

        return;
        Task<int> ThrowAggregateExceptionAsync() => Task.FromException<int>(aggregateException);
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void RunSyncReturnWithValueTask_WhenThrowAggregateException_ShouldThrowSameExceptionWithAndWithoutAsyncContest()
    {
        var oldContext = SynchronizationContext.Current;

        var aggregateException = new AggregateException(new List<Exception>{new TestException()});
        try
        {
            SynchronizationContext.SetSynchronizationContext(null);
            var withoutContext = Assert.ThrowsAny<Exception>(() => AsyncHelpers.RunSync(ThrowAggregateExceptionAsync));

            SynchronizationContext.SetSynchronizationContext(new SimpleSynchronizationContext());
            var withContext = Assert.ThrowsAny<Exception>(() => AsyncHelpers.RunSync(ThrowAggregateExceptionAsync));
        
            Assert.Equal(withoutContext.GetType(), withContext.GetType());
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(oldContext);
        }

        return;
        ValueTask<int> ThrowAggregateExceptionAsync() => ValueTask.FromException<int>(aggregateException);
    }
    
    private class TestException : Exception
    {
            
    }
        
    private class SimpleSynchronizationContext : SynchronizationContext
    {
        public override void Post(SendOrPostCallback d, object state) => d(state);
    }
}
