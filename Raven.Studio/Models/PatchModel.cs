using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Studio.Behaviors;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class PatchModel : ViewModel, IAutoCompleteSuggestionProvider
	{
		private PatchOnOptions patchOn;
		private string originalDoc;
		private string newDoc;
		public PatchOnOptions PatchOn
		{
			get { return patchOn; }
			set
			{
				patchOn = value;
				OnPropertyChanged(() => PatchOn);
			}
		}

		public string OriginalDoc
		{
			get { return originalDoc; }
			set
			{
				originalDoc = value;
				OnPropertyChanged(() => OriginalDoc);
			}
		}

		public string NewDoc
		{
			get { return newDoc; }
			set
			{
				newDoc = value;
				OnPropertyChanged(() => NewDoc);
			}
		}

		public string SelectedItem { get; set; }
		public string Script { get; set; }
		public const string CollectionsIndex = "Raven/DocumentsByEntityName";
		public ICommand Execute { get { return new ExecutePatchCommand(this); } }
		public ICommand Test { get { return new TestPatchCommand(this); } }

		public Task<IList<object>> ProvideSuggestions(string enteredText)
		{
			switch (PatchOn)
			{
				case PatchOnOptions.Document:
					return ApplicationModel.Database.Value.AsyncDatabaseCommands.StartsWithAsync(SelectedItem, 0, 25, metadataOnly: true)
						.ContinueWith(t => (IList<object>) t.Result.Select(d => d.Key).Cast<object>().ToList());

				case PatchOnOptions.Collection:
					return ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.GetTermsCount(
				CollectionsIndex, "Tag", "", 100)
				.ContinueOnSuccess(collections => (IList<object>)collections.OrderByDescending(x => x.Count)
											.Where(x => x.Count > 0)
											.Select(col => col.Name).Cast<object>().ToList());

				case PatchOnOptions.Index:
					return ApplicationModel.Database.Value.AsyncDatabaseCommands.GetIndexNamesAsync(0, 25)
						.ContinueWith(t => (IList<object>) t.Result.Where(s => s.StartsWith(enteredText, StringComparison.InvariantCultureIgnoreCase)).Cast<object>().ToList());

				default:
					return null;
			}
		}
	}

	public class TestPatchCommand : Command
	{
		private readonly PatchModel patchModel;

		public TestPatchCommand(PatchModel patchModel)
		{
			this.patchModel = patchModel;
		}

		public override void Execute(object parameter)
		{
			var request = new ScriptedPatchRequest {Script = patchModel.Script};

			switch (patchModel.PatchOn)
			{
				case PatchOnOptions.Document:
					ApplicationModel.Database.Value.AsyncDatabaseCommands.GetAsync(patchModel.SelectedItem).
						ContinueOnSuccessInTheUIThread(doc => patchModel.OriginalDoc = doc.ToJson().ToString());
					//Todo: get a sample of the doc after changes (don't save the changes) and save it to NewDoc
					break;

				case PatchOnOptions.Collection:
					//Todo: get the first item from the query and show it in OriginalDoc, and get a sample of the doc after changes (don't save the changes) and save it to NewDoc
					break;

				case PatchOnOptions.Index:
					//Todo: get the first item from the query and show it in OriginalDoc, and get a sample of the doc after changes (don't save the changes) and save it to NewDoc
					break;
			}
		}
	}

	public class ExecutePatchCommand : Command
	{
		private readonly PatchModel patchModel;

		public ExecutePatchCommand(PatchModel patchModel)
		{
			this.patchModel = patchModel;
		}

		public override void Execute(object parameter)
		{
			var request = new ScriptedPatchRequest {Script = patchModel.Script};

			switch (patchModel.PatchOn)
			{
				case PatchOnOptions.Document:
					var commands = new ICommandData[1];
					commands[0] = new ScriptedPatchCommandData
					{
						Patch = request,
						Key = patchModel.SelectedItem
					};

					ApplicationModel.Database.Value.AsyncDatabaseCommands.BatchAsync(commands);
					break;

				case PatchOnOptions.Collection:
					ApplicationModel.Database.Value.AsyncDatabaseCommands.UpdateByIndex(PatchModel.CollectionsIndex,
					                                                                    new IndexQuery
					                                                                    {Query = "Tag:" + patchModel.SelectedItem},
					                                                                    request);
					break;

				case PatchOnOptions.Index:
					ApplicationModel.Database.Value.AsyncDatabaseCommands.UpdateByIndex(patchModel.SelectedItem, new IndexQuery(),
					                                                                    request);
					break;
			}
		}
	}

	public enum PatchOnOptions
	{
		Document,
		Collection,
		Index
	}
}
