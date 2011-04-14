using Raven.Abstractions.Data;

namespace Raven.Studio.Commands
{
	using System.Windows;

    public class CopyServerError
	{
		public void Execute(ServerError error)
		{
			var msg = string.Format(
@"{0}
  Document:	{1}
  Index:	{2}
  When:		{3}",
			                        error.Error,
			                        error.Document,
			                        error.Index,
			                        error.Timestamp);
			Clipboard.SetText(msg);
		}

		public bool CanExecute(ServerError error) { return error != null; }
	}
}