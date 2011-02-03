namespace Raven.Studio.Framework
{
	using System;
	using System.Threading.Tasks;

	public static class TaskExtensions
	{
		public static void ContinueOnSuccess<T>(this Task<T> task, Action<Task<T>> onSuccess)
		{
			task.ContinueWith(child =>
			{
				if (child.IsFaulted)
				{
					throw new NotImplementedException();
				}
				else
				{
					onSuccess(child);
				}
			});
		}

		public static void ContinueOnSuccess(this Task task, Action<Task> onSuccess)
		{
			task.ContinueWith(child =>
			                  	{
									if (child.IsFaulted)
									{
										throw new NotImplementedException();
									}
									else
									{
										onSuccess(child);
									}
			                  	});
		}
	}
}