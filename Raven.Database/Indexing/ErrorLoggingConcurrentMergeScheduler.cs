using System;
using Lucene.Net.Index;
using Raven.Abstractions.Logging;

namespace Raven.Database.Indexing
{
	public class ErrorLoggingConcurrentMergeScheduler : ConcurrentMergeScheduler
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		protected override void HandleMergeException(System.Exception exc)
		{
			try
			{
				base.HandleMergeException(exc);
			}
			catch (Exception e)
			{
				log.WarnException("Concurrent merge failed", e);
			}
		}
	}
}
