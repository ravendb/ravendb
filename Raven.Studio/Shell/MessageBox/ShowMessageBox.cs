namespace Raven.Studio.Shell.MessageBox
{
    using System;

    public delegate void ShowMessageBox(
		string message, string title, MessageBoxOptions options = MessageBoxOptions.Ok, Action<IMessageBox> callback = null);
}