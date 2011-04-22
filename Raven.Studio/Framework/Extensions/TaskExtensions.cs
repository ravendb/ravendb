using System.Reflection;
namespace Raven.Studio.Framework.Extensions

{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Net;
	using System.Threading.Tasks;
	using Caliburn.Micro;
	using Client.Extensions;
	using Shell;

	public static class TaskExtensions
	{

		public static Task ContinueWith<T>(this Task<T> task, Action<Task<T>> onSuccess, Action<Task<T>> onFault)
		{
			return task.ContinueWith(child =>
			{
				if (child.IsFaulted)
				{
					onFault(child);
				}
				else
				{
					onSuccess(child);
				}
			});
		}

		public static Task ContinueWith(this Task task, Action<Task> onSuccess, Action<Task> onFault)
		{
			return task.ContinueWith(child =>
			{
				if (child.IsFaulted)
				{
					onFault(child);
				}
				else
				{
					onSuccess(child);
				}
			});
		}

		public static Task ContinueOnSuccess<T>(this Task<T> task, Action<Task<T>> onSuccess)
		{
			return task.ContinueWith(child =>
			{
				if (child.IsFaulted)
				{
					NotifyUserOfError(child, onSuccess.Method);
				}
				else
				{
					HideErrorOnSuccess(onSuccess.Method);
					onSuccess(child);
				}
			});
		}

		private static void HideErrorOnSuccess(MethodInfo source)
		{
			Execute.OnUIThread(() =>
			{
				if (ErrorViewModel.Current != null && 
				    ErrorViewModel.Current.CurrentErrorSource == source)
				{
					ErrorViewModel.Current.TryClose();
				}
			});
		}

		public static Task ContinueOnSuccess(this Task task, Action<Task> onSuccess)
		{
			return task.ContinueWith(child =>
			                  	{
									if (child.IsFaulted)
									{
										NotifyUserOfError(child, onSuccess.Method);
									}
									else
									{
										HideErrorOnSuccess(onSuccess.Method); 
										onSuccess(child);
									}
			                  	});
		}

		static void NotifyUserOfError(Task child, MethodInfo errorSource)
		{
			Execute.OnUIThread(()=>
			{
				if (ErrorViewModel.Current != null)
					return;
				
				IoC.Get<IWindowManager>()
						.ShowDialog(new ErrorViewModel
						{
							CurrentErrorSource = errorSource,
							Message = "Unable to connect to server!",
							Details = GetErrorDetails(child.Exception)
						});
			});
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

		public static void ExecuteInSequence(this IEnumerable<Task> tasks, Action<bool> callback, Action<Exception> handleException = null)
		{
			ExecuteNextTask(tasks.GetEnumerator(), callback, handleException);
		}

		static void ExecuteNextTask(IEnumerator<Task> enumerator, Action<bool> callback, Action<Exception> handleException)
		{
			try
			{
				bool moveNextSucceeded = enumerator.MoveNext();

				if (!moveNextSucceeded)
				{
					enumerator.Dispose();
					if (callback != null) 
						callback(true);
					return;
				}
			
				enumerator
					.Current
					.ContinueWith(x =>
					{
						if (x.Exception != null)
						{
							enumerator.Dispose(); 
							HandleError(callback, handleException, x.Exception);
							return;
						}
						ExecuteNextTask(enumerator, callback, handleException);
					});
			}
			catch (Exception e)
			{
				enumerator.Dispose(); 
				HandleError(callback, handleException, e);
			}
		}

		private static void HandleError(Action<bool> callback, Action<Exception> handleException, Exception e)
		{
			if (handleException != null)
			{
				handleException(e);
				if (callback != null)
					callback(false);
			}
			else new TargetInvocationException("Could not execute a set of tasks properly", e);
		}
	}
}