using System.Collections.ObjectModel;
using System.Windows.Input;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;

namespace Raven.Studio.Models
{
	public class TransformerDefinitionModel : PageViewModel, IHasPageTitle
	{
		public TransformerDefinition Transformer { get; private set; }

		public TransformerDefinitionModel()
		{
			ModelUrl = "/transformers/";
			Transformer = new TransformerDefinition();
            Errors = new ObservableCollection<TransformerDefinitionError>();
		}

	    public ObservableCollection<TransformerDefinitionError> Errors { get; private set; }

        public bool IsShowingErrors
        {
            get { return isShowingErrors; }
            set
            {
                isShowingErrors = value;
                OnPropertyChanged(() => IsShowingErrors);
            }
        }

	    public ICommand SaveTransformer
		{
			get { return new SaveTransformerCommand(this); }
		}
		public ICommand DeleteTransformer
		{
			get
			{
				return new DeleteTransformerCommand(this);
			}
		}
		public bool IsNewTransformer { get; private set; }

		private string header;
	    private bool isShowingErrors;

	    public string Header
		{
			get { return header; }
			private set
			{
				header = value;
				OnPropertyChanged(() => Header);
			}
		}

        private void ReportDefinitionError(string message)
        {
            Errors.Add(new TransformerDefinitionError() { Message = message });

            IsShowingErrors = true;
        }

        private void ClearDefinitionErrors()
        {
            Errors.Clear();
        }


		public override void LoadModelParameters(string parameters)
		{
			var urlParser = new UrlParser(parameters);
			if (urlParser.GetQueryParam("mode") == "new")
			{
				IsNewTransformer = true;
				Header = "New Transformer";

				UpdateFromTransformer(new TransformerDefinition());

				return;
			}

			var name = urlParser.Path;
			if (string.IsNullOrWhiteSpace(name))
				HandleTransformerNotFound(null);

			Header = name;
			OriginalName = name;
			IsNewTransformer = false;

			DatabaseCommands.GetTransformerAsync(name)
				.ContinueOnUIThread(task =>
				{
					if (task.IsFaulted || task.Result == null)
					{
						HandleTransformerNotFound(name);
						return;
					}

					UpdateFromTransformer(task.Result);
				}).Catch();
		}

		private string OriginalName { get; set; }

		private void UpdateFromTransformer(TransformerDefinition transformerDefinition)
		{
			Transformer = transformerDefinition;

			OnEverythingChanged();
		}

		private static void HandleTransformerNotFound(string name)
		{
			if (string.IsNullOrWhiteSpace(name) == false)
			{
				var notification = new Notification(string.Format("Could not find '{0}' transformer", name), NotificationLevel.Warning);
				ApplicationModel.Current.AddNotification(notification);
			}
			UrlUtil.Navigate("/transformers");
		}

		public string PageTitle
		{
			get { return "Edit Transformer"; }
		}

		private class SaveTransformerCommand : Command
		{
			private readonly TransformerDefinitionModel transformer;

			public SaveTransformerCommand(TransformerDefinitionModel transformer)
			{
				this.transformer = transformer;
			}

			public override void Execute(object parameter)
			{
			    transformer.ClearDefinitionErrors();

				if (string.IsNullOrWhiteSpace(transformer.Transformer.Name))
				{
					transformer.ReportDefinitionError("Transformer must have a name");
					return;
				}

                if (string.IsNullOrWhiteSpace(transformer.Transformer.TransformResults))
                {
                    transformer.ReportDefinitionError("Transform must contain a C# LINQ query or Query expression");
                    return;
                }

				if (transformer.IsNewTransformer == false && transformer.OriginalName != transformer.Transformer.Name)
				{
					if (AskUser.Confirmation("Can not rename and transformer",
											 "If you wish to save a new transformer with this new name press OK, to cancel the save command press Cancel") == false)
					{
						ApplicationModel.Current.Notifications.Add(new Notification("Transformer Not Saved"));
						return;
					}
				}

				ApplicationModel.Current.AddNotification(new Notification("saving transformer " + transformer.Transformer.Name));
				DatabaseCommands.PutTransformerAsync(transformer.Transformer.Name, transformer.Transformer)
					.ContinueOnSuccess(() =>
					{
						ApplicationModel.Current.AddNotification(
							new Notification("transformer " + transformer.Transformer.Name + " saved"));
						PutTransformerNameInUrl(transformer.Transformer.Name);

					    transformer.IsShowingErrors = false;
					})
                    .Catch(ex =>
                    {
                        var indexException = ex.ExtractSingleInnerException() as TransformCompilationException;
                        if (indexException != null)
                        {
                            transformer.ReportDefinitionError(indexException.Message);
                            return true;
                        }
                        return false;
                    }); ;
			}

			private void PutTransformerNameInUrl(string name)
			{
				if (transformer.IsNewTransformer || transformer.Header != name)
					UrlUtil.Navigate("/transformers/" + name, true);
			}
		}



	    private class DeleteTransformerCommand : Command
		{
			private readonly TransformerDefinitionModel model;

			public DeleteTransformerCommand(TransformerDefinitionModel model)
			{
				this.model = model;
			}

			public override void Execute(object parameter)
			{
				AskUser.ConfirmationAsync("Confirm Delete", "Really delete '" + model.Transformer.Name + "' transformer?")
					.ContinueWhenTrue(DeleteTransformer);
			}

			private void DeleteTransformer()
			{
				DatabaseCommands
					.DeleteTransformerAsync(model.Transformer.Name)
					.ContinueOnUIThread(t =>
					{
						if (t.IsFaulted)
						{
							ApplicationModel.Current.AddErrorNotification(t.Exception, "transformer " + model.Transformer.Name + " could not be deleted");
						}
						else
						{
							ApplicationModel.Current.AddInfoNotification("transformer " + model.Transformer.Name + " successfully deleted");
							UrlUtil.Navigate("/transformers");
						}
					});
			}
		}
	}

	public class TransformerDefinitionError
	{
        public string Message { get; set; }
	}
}
