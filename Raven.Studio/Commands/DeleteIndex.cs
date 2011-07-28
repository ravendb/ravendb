using Raven.Studio.Features.Indexes;
using Raven.Studio.Framework.Extensions;

namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
	using System.Linq;
	using Caliburn.Micro;
	using Messages;
	using Plugins;
	using Shell.MessageBox;

	[Export]
	public class DeleteIndex
	{
		readonly IEventAggregator events;
		readonly IServer server;
		readonly ShowMessageBox showMessageBox;

		[ImportingConstructor]
		public DeleteIndex(IServer server, IEventAggregator events, ShowMessageBox showMessageBox)
		{
			this.server = server;
			this.events = events;
			this.showMessageBox = showMessageBox;
		}

		public bool CanExecute(EditIndexViewModel index)
		{
			return index != null && string.IsNullOrWhiteSpace(index.Name) == false;
		}

		public void Execute(EditIndexViewModel index)
		{
			showMessageBox(
				string.Format("Are you sure that you want to delete this index? ({0})", index.Name),
				"Confirm Deletion",
				MessageBoxOptions.OkCancel,
				box => {
					if (box.WasSelected(MessageBoxOptions.Ok))
					{
						ExecuteDeletion(index);
					}
				});
		}

		void ExecuteDeletion(EditIndexViewModel index)
		{
			events.Publish(new WorkCompleted("removing index " + index.Name));
			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.DeleteIndexAsync(index.Name)
					.ContinueOnSuccess(task =>
										{
											events.Publish(new WorkCompleted("removing index " + index.Name));
											events.Publish(new IndexUpdated { Index = index, IsRemoved = true });
										});
			}
		}
	}
}