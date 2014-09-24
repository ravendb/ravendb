using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.FileSystem;
using Xunit;

namespace Raven.Tests.Helpers
{
    public static class TaskAssert
    {
        public static T Throws<T>(Func<Task> f) where T : Exception
        {
            return Assert.Throws<T>(() =>
            {
                try
                {
                    f().Wait();
                }
                catch (AggregateException e)
                {
                    return e.SimplifyException();
                }
                catch (Exception ex)
                {
                    return ex;
                }

                return null;
            });
        }

        public static T Throws<T>(Func<Task<T>> f) where T : Exception
        {
            return Assert.Throws<T>(() =>
            {
                try
                {
                    f().Wait();
                }
                catch (AggregateException e)
                {
                    return e.SimplifyException();
                }
                catch (Exception ex)
                {
                    return ex;
                }

                return null;
            });
        }
    }
}
