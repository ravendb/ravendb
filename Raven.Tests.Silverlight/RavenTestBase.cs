namespace Raven.Tests.Silverlight
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Threading.Tasks;
    using Microsoft.Silverlight.Testing;

    public abstract class RavenTestBase : SilverlightTest
    {
        protected const int Port = 8080;
        protected const string Url = "http://localhost:";

        protected static string GenerateNewDatabaseName()
        {
            return Guid.NewGuid().ToString();
        }

        protected Task Delay(int milliseconds)
        {
            return TaskEx.Delay(TimeSpan.FromMilliseconds(milliseconds));
        }

        public void ExecuteTest(MethodInfo test)
        {
            var tasks = (IEnumerable<Task>) test.Invoke(this, new object[] {});
            IEnumerator<Task> enumerator = tasks.GetEnumerator();
            ExecuteTestStep(enumerator);
        }

        private void ExecuteTestStep(IEnumerator<Task> enumerator)
        {
            bool moveNextSucceeded = false;
            try
            {
                moveNextSucceeded = enumerator.MoveNext();
            }
            catch (Exception ex)
            {
                EnqueueTestComplete();
                return;
            }

            if (moveNextSucceeded)
            {
                try
                {
                    Task next = enumerator.Current;
                    EnqueueConditional(() => next.IsCompleted || next.IsFaulted);
                    EnqueueCallback(() => ExecuteTestStep(enumerator));
                }
                catch (Exception ex)
                {
                    EnqueueTestComplete();
                    return;
                }
            }
            else EnqueueTestComplete();
        }
    }
}