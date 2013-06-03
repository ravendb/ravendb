using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Silverlight.Testing;
using Raven.Client.Extensions;

namespace Raven.Tests.Silverlight.UnitTestProvider
{
	public abstract class AsynchronousTaskTest : SilverlightTest
	{
		internal void ExecuteTest(MethodInfo test)
		{
			var tasks = (IEnumerable<Task>)test.Invoke(this, new object[] { });
			IEnumerator<Task> enumerator = tasks.GetEnumerator();
			ExecuteTestStep(enumerator);
		}

		internal void ExecuteTaskTest(MethodInfo test)
		{
			var task = (Task)test.Invoke(this, new object[] { });
			EnqueueConditional(() => task.IsCompleted || task.IsFaulted);
			EnqueueCallback(() =>
								{
									if (task.IsFaulted) throw task.Exception.InnerException;
								});
			EnqueueTestComplete();
		}

		private void ExecuteTestStep(IEnumerator<Task> enumerator)
		{
			bool moveNextSucceeded = enumerator.MoveNext();

			if (moveNextSucceeded)
			{
				Task next = enumerator.Current;
				EnqueueConditional(() => next.IsCompleted || next.IsFaulted);
				EnqueueCallback(() => ExecuteTestStep(enumerator));
			}
			else EnqueueTestComplete();
		}
	}
}