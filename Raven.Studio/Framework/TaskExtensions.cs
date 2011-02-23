namespace Raven.Studio.Framework
{
	using System;
	using System.Threading.Tasks;
	using Caliburn.Micro;
	using Client.Extensions;
	using Shell;

	public static class TaskExtensions
	{
		public static void ContinueOnSuccess<T>(this Task<T> task, Action<Task<T>> onSuccess)
		{
			task.ContinueWith(child =>
			{
				if (child.IsFaulted)
				{
					NotifyUserOfError(child);
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
										NotifyUserOfError(child);
									}
									else
									{
										onSuccess(child);
									}
			                  	});
		}

		static void NotifyUserOfError(Task child)
		{
			Execute.OnUIThread(()=> IoC
			        .Get<IWindowManager>()
			        .ShowDialog(new ErrorViewModel
			                    	{
			                    		Message = "Unable to connect to server!", 
										Details = GetErrorDetails(child.Exception)
			                    	}));
		}

		static string GetErrorDetails(AggregateException x)
		{
			var single = x.ExtractSingleInnerException();
			while (single !=null && single.InnerException != null)
			{
				single = single.InnerException;
			}

			//if(single.Message == "Security error.") 
			//    return "Silverlight is not able to connect to the specified address for the server. Is it running?";
			
			return ( single == null) 
			       	? null
			       	: single.Message;
		}
	}
}