using System;

namespace Raven.Smuggler.Database
{
	public class DatabaseSmugglerNotifications
	{
		public EventHandler<string> OnDocumentRead = (sender, key) => { };

        public EventHandler<string> OnDocumentWrite = (sender, key) => { };

        public EventHandler<string> OnProgress = (sender, message) => { };

		public void ShowProgress(string format, params object[] args)
		{
			try
			{
				var message = string.Format(format, args);
				Console.WriteLine(message);
				OnProgress(this, message);
			}
			catch (FormatException e)
			{
				throw new FormatException("Input string is invalid: " + format + Environment.NewLine + string.Join(", ", args), e);
			}
		}
	}
}