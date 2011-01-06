using System;
using System.Threading.Tasks;
using log4net.Appender;
using log4net.Core;
using log4net.Util;

namespace Raven.Database.Impl.Logging
{
	public class AsyncBufferingAppender : AppenderSkeleton
	{
		private readonly AppenderAttachedImpl appenders = new AppenderAttachedImpl();

		public void AddAppender(IAppender appender)
		{
			appenders.AddAppender(appender);
		}

		protected override void OnClose()
		{
			appenders.RemoveAllAppenders();
			base.OnClose();
		}

		protected override void Append(LoggingEvent loggingEvent)
		{
			Task.Factory.StartNew(() => { appenders.AppendLoopOnAppenders(loggingEvent); });
		}
	}
}