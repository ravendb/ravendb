//-----------------------------------------------------------------------
// <copyright file="ParallelExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.Extensions
{
    internal static class ParallelExtensions
    {
        public static void WaitAll(this IEnumerable<Task> tasks)
        {
            try
            {
                Task.WaitAll(tasks.ToArray());
            }
            catch (Exception ex)
            {
                //when task takes exception it wraps in aggregate exception, if in continuation
                //then could be double wrapped, etc. This should always get us the original
                while (true)
                {
                    if (ex.InnerException == null || !(ex is AggregateException))
                    {
                        throw PreserveStackTrace(ex);
                    }
                    ex = ex.InnerException;
                }
            }
        }

        private static Exception PreserveStackTrace(Exception exception)
        {
            const BindingFlags Flags = BindingFlags.Instance | BindingFlags.NonPublic;

            typeof(Exception).InvokeMember("InternalPreserveStackTrace", Flags, exception);
            return exception;
        }
    }
}
