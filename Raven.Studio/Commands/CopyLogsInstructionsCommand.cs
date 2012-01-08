using System.Windows;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
	public class CopyLogsInstructionsCommand : Command
	{
		public override void Execute(object parameter)
		{
			var textFile = (string) parameter;

			Clipboard.SetText(textFile);
		}
	}
}