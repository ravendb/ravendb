using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class DeleteDocumentCommand : Command
	{
		private readonly string key;
		private readonly bool navigateOnSuccess;

		public DeleteDocumentCommand(string key, bool navigateOnSuccess)
		{
			this.key = key;
			this.navigateOnSuccess = navigateOnSuccess;
		}

		public override bool CanExecute(object parameter)
		{
			return string.IsNullOrWhiteSpace(key) == false;
		}

		public override void Execute(object parameter)
		{
			AskUser.ConfirmationAsync("Confirm Delete", string.Format("Really delete {0} ?", key))
				.ContinueWhenTrue(DeleteDocument);
		}

		private void DeleteDocument()
		{
			DatabaseCommands.DeleteDocumentAsync(key)
				.ContinueOnSuccessInTheUIThread(() =>
				                                	{
				                                		ApplicationModel.Current.AddNotification(new Notification(string.Format("Document {0} was deleted", key)));
														if (navigateOnSuccess)
															UrlUtil.Navigate("/documents");
				                                	})
				.Catch();
		}
	}
}