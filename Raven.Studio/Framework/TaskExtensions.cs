namespace Raven.Studio.Framework
{
	using System;
	using System.IO;
	using System.Net;
	using System.Threading.Tasks;
	using Caliburn.Micro;
	using Client.Extensions;
	using Shell;

	public static class TaskExtensions
	{
		public static Task ContinueOnSuccess<T>(this Task<T> task, Action<Task<T>> onSuccess)
		{
			return task.ContinueWith(child =>
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

		public static Task ContinueOnSuccess(this Task task, Action<Task> onSuccess)
		{
			return task.ContinueWith(child =>
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

		    var webException = single as WebException;
            if(webException != null)
            {
                var httpWebResponse = webException.Response as HttpWebResponse;
                if(httpWebResponse != null)
                {
                    using (var reader = new StreamReader(httpWebResponse.GetResponseStream()))
                    {
                        return "The remote server returned an error: " + httpWebResponse.StatusDescription +
                               Environment.NewLine + 
                               reader.ReadToEnd();
                    }

                }
            }

		    return ( single == null) 
			       	? null
			       	: single.Message;
		}
	}
}