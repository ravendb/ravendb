// -----------------------------------------------------------------------
//  <copyright file="TaskCompletionSourceFactory.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Reflection;
using System.Threading.Tasks;

namespace Raven.Client.Util
{
    internal static class TaskCompletionSourceFactory
    {
#if !DNXCORE50
        static readonly FieldInfo StateField =
            typeof(Task).GetField("m_stateFlags", BindingFlags.NonPublic | BindingFlags.Instance);

#endif

        internal static TaskCompletionSource<T> Create<T>(TaskCreationOptions options = TaskCreationOptions.None)
        {
#if !DNXCORE50
            //This lovely hack forces the task scheduler to run continuations asynchronously,
            //see https://stackoverflow.com/questions/22579206/how-can-i-prevent-synchronous-continuations-on-a-task/22588431#22588431
            var tcs = new TaskCompletionSource<T>(options);
            const int TASK_STATE_THREAD_WAS_ABORTED = 134217728;
            StateField.SetValue(tcs.Task, (int)StateField.GetValue(tcs.Task) | TASK_STATE_THREAD_WAS_ABORTED);
            return tcs;
#else
            return new TaskCompletionSource<T>(options | TaskCreationOptions.RunContinuationsAsynchronously);
#endif
        }
    }
}