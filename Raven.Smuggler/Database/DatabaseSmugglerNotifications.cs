using System;

namespace Raven.Smuggler.Database
{
	public class DatabaseSmugglerNotifications
	{
		public EventHandler<string> DocumentRead = (sender, key) => { };

		public void ShowProgress(string format, params object[] args)
		{
			try
			{
				Console.WriteLine(format, args);
			}
			catch (FormatException e)
			{
				throw new FormatException("Input string is invalid: " + format + Environment.NewLine + string.Join(", ", args), e);
			}
		}
	}
}