using System;
using System.Collections.Generic;
using System.Threading;
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

            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            var withContext = Assert.ThrowsAny<Exception>(() => AsyncHelpers.RunSync(() => throw testException));
                
            Assert.Equal(withoutContext.GetType(), withContext.GetType());
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(oldContext);
        }
    }
        
    [RavenFact(RavenTestCategory.ClientApi)]
    public void RunSync_WhenThrowAggregateException_ShouldThrowSameExceptionWithAndWithoutAsyncContest()
    {
        var oldContext = SynchronizationContext.Current;

        var aggregateException = new AggregateException(new List<Exception>{new TestException()});
        try
        {
            SynchronizationContext.SetSynchronizationContext(null);
            var withoutContext = Assert.ThrowsAny<Exception>(() => AsyncHelpers.RunSync(() => throw aggregateException));

            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            var withContext = Assert.ThrowsAny<Exception>(() => AsyncHelpers.RunSync(() => throw aggregateException));
        
            Assert.Equal(withoutContext.GetType(), withContext.GetType());
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(oldContext);
        }
    }
        
    private class TestException : Exception
    {
            
    }
        
    private class SimpleSynchronizationContext : SynchronizationContext
    {
        public override void Post(SendOrPostCallback d, object state) => d(state);
    }
}
