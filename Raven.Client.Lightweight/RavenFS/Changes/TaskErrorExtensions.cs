using System;
using System.Threading.Tasks;

namespace Raven.Client.RavenFS.Changes
{
	internal static class TaskErrorExtensions
	{
		public static async Task ObserveException(this Task self)
		{
			// this merely observe the exception task, nothing else
			try
			{
				await self;
			}
			catch (Exception e)
			{
				GC.KeepAlive(e);
			}
		}
	}
}