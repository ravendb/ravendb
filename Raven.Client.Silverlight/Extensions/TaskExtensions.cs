using System;
using System.Threading.Tasks;

namespace Raven.Client.Silverlight.Extensions
{
	public static class TaskExtensions
	{
		public static Task<T> AddUrlIfFaulting<T>(this Task<T> parent, Uri uri)
		{
			return parent.ContinueWith(task =>
			                           	{
			                           		if (task.IsFaulted)
			                           		{
			                           			task.Exception.Data["Url"] = uri;
			                           		}

			                           		return task;
			                           	})
				.Unwrap();
		}
	}
}