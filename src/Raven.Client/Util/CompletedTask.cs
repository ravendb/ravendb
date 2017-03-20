using System;
using System.Threading.Tasks;

namespace Raven.Client.Util
{
    public class CompletedTask : CompletedTask<object>
    {
        public new Task Task => base.Task;

        public static CompletedTask<T> With<T>(T result)
        {
            return new CompletedTask<T>(result);
        }

        public CompletedTask()
        {
        }

        public CompletedTask(Exception error) : base(error)
        {
        }
    }


    public class CompletedTask<T>
    {
        private readonly Exception _error;
        public readonly T Result;

        public CompletedTask() : this(default(T)) { }

        public CompletedTask(T result)
        {
            Result = result;
        }

        public CompletedTask(Exception error)
        {
            _error = error;
        }

        public Task<T> Task
        {
            get
            {
                var tcs = new TaskCompletionSource<T>();
                if (_error == null)
                    tcs.SetResult(Result);
                else
                    tcs.SetException(_error);
                return tcs.Task;
            }
        }

        public static implicit operator Task<T>(CompletedTask<T> t)
        {
            return t.Task;
        }

        public static implicit operator Task(CompletedTask<T> t)
        {
            return t.Task;
        }
    }
}
