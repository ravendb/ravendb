using System.Windows;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
	public class CopyToClipboardCommand : Command
	{
		private string text;

		public override bool CanExecute(object parameter)
		{
			text = parameter as string;
			return text != null;
		}

		public override void Execute(object parameter)
		{
			Clipboard.SetText(text);
		}
	}
}