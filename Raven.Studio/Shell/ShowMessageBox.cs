namespace Raven.Studio.Shell
{
	using System;

	public delegate void ShowMessageBox(
		string message, string title, MessageBoxOptions options = MessageBoxOptions.Ok, Action<IMessageBox> callback = null);
}