using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.FileSystem;
using Xunit;

namespace RavenFS.Tests
{
    public static class TaskAssert
    {
        public static void Throws<T>(Func<Task> f) where T : Exception
        {
            Assert.Throws<T>(() =>
            {                
                try
                {
                    f().Wait();
                }
                catch (AggregateException e)
                {
                    throw e.SimplifyException();
                }
            });
        }

    }
}
