using System.Collections.ObjectModel;
using System.Windows.Input;
using Raven.Abstractions.Indexing;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class TransformersModel : PageViewModel
	{
		public ObservableCollection<TransformerDefinition> Transformers { get; set; }
		public ICommand DeleteTransformer
		{
			get { return new DeleteTransformerCommand(this); }
		}
		public TransformerDefinition ItemSelection { get; set; }

		public TransformersModel()
		{
			ModelUrl = "/transformers";
			Transformers = new ObservableCollection<TransformerDefinition>();
			ApplicationModel.Current.Server.Value.RawUrl = "databases/" +
																	   ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name +
																	   "/transformers";

			DatabaseCommands.GetTransformersAsync(0, 256).ContinueOnSuccessInTheUIThread(transformers =>
			{
				Transformers = new ObservableCollection<TransformerDefinition>(transformers);
				OnPropertyChanged(() => Transformers);
			} );
		}

		private class DeleteTransformerCommand : Command
		{
			private readonly TransformersModel model;

			public DeleteTransformerCommand(TransformersModel model)
			{
				this.model = model;
			}

			public override void Execute(object parameter)
			{
				AskUser.ConfirmationAsync("Confirm Delete", "Really delete '" + model.ItemSelection.Name + "' transformer?")
					.ContinueWhenTrue(DeleteTransformer);
			}

			private void DeleteTransformer()
			{
				DatabaseCommands
					.DeleteTransformerAsync(model.ItemSelection.Name)
					.ContinueOnUIThread(t =>
					{
						if (t.IsFaulted)
						{
							ApplicationModel.Current.AddErrorNotification(t.Exception, "transformer " + model.ItemSelection.Name + " could not be deleted");
						}
						else
						{
							ApplicationModel.Current.AddInfoNotification("transformer " + model.ItemSelection.Name + " successfully deleted");
							UrlUtil.Navigate("/transformers");
						}
					});
			}
		}
	}
}
