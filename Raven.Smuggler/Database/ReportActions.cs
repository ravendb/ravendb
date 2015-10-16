using System;

namespace Raven.Smuggler.Database
{
	public class ReportActions
	{
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