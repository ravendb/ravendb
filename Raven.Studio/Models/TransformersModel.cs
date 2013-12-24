using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class TransformersModel : PageViewModel
	{
		private Group selectedGroup;
		public ObservableCollection<Group> GroupedTransformers { get; private set; }
		public ObservableCollection<TransformerDefinition> Transformers { get; set; }
		private string currentDatabase;
		private string currentSearch;
		public Observable<bool> UseGrouping { get; set; }

		public ICommand DeleteTransformer
		{
			get { return new DeleteTransformerCommand(this); }
		}

		public GroupItem ItemSelection { get; set; }

		public ICommand DeleteAllTransformers
		{
			get
			{
				return new ActionCommand(() => AskUser.ConfirmationAsync("Confirm Delete", "Really delete all transformers?")
					.ContinueWhenTrue(DeleteTransformers));
			}
		}

		private void DeleteTransformers()
		{
			var tasks =
				Transformers.Select(transformerDefinition => DatabaseCommands.DeleteTransformerAsync(transformerDefinition.Name))
					.ToList();
			TaskEx.WhenAll(tasks)
				.ContinueOnUIThread(t =>
				{
					if (t.IsFaulted)
					{
						ApplicationModel.Current.AddErrorNotification(t.Exception, "not all transformers could be deleted");
					}
					else
					{
						ApplicationModel.Current.AddInfoNotification("all transformers were successfully deleted");
						UrlUtil.Navigate("/transformers");
					}
				});
		}

		public TransformersModel()
		{
			ModelUrl = "/transformers";
			Transformers = new ObservableCollection<TransformerDefinition>();
			GroupedTransformers = new ObservableCollection<Group>();
			ApplicationModel.Current.Server.Value.RawUrl = "databases/" +
														   ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name +
														   "/transformers";
			ApplicationModel.Database.PropertyChanged += (sender, args) => TimerTickedAsync();
			SearchText = new Observable<string>();
			UseGrouping = new Observable<bool> { Value = true };
			SearchText.PropertyChanged += (sender, args) => TimerTickedAsync();

		}

		public override Task TimerTickedAsync()
		{
			return DatabaseCommands
				.GetStatisticsAsync()
				.ContinueOnSuccessInTheUIThread(UpdateTransformersList);
		}

		protected void UpdateTransformersList()
		{
			DatabaseCommands.GetTransformersAsync(0, 256).ContinueOnSuccessInTheUIThread(transformers =>
			{
				Transformers.Clear();

				CleanGroup();

				if (string.IsNullOrWhiteSpace(SearchText.Value))
					Transformers = new ObservableCollection<TransformerDefinition>(transformers);

				else
				{
					Transformers = new ObservableCollection<TransformerDefinition>(transformers
						.Where(definition => definition.Name.IndexOf(SearchText.Value, StringComparison.InvariantCultureIgnoreCase) != -1));
					
				}

				foreach (var transformer in Transformers.OrderBy(definition => definition.Name))
				{
					var groupName = DetermineName(transformer);
					var groupItem =
						GroupedTransformers.FirstOrDefault(
							@group => string.Equals(@group.GroupName, groupName, StringComparison.OrdinalIgnoreCase));
					if (groupItem == null)
					{
						groupItem = new Group(groupName);
						GroupedTransformers.Add(groupItem);
					}

					groupItem.Items.Add(new TransformerItem { GroupName = groupName, Name = transformer.Name, Transformer = transformer });
				}

				OnPropertyChanged(() => GroupedTransformers);
				OnPropertyChanged(() => Transformers);
			});
		}

		private string DetermineName(TransformerDefinition transformer)
		{
			if (UseGrouping.Value == false)
				return "Transformers";

			var name = transformer.Name;
			if (name.Contains("/") == false)
				return "No Group";
			var groups = name.Split('/');
			return groups[0];
		}

		private void CleanGroup()
		{
			if (currentDatabase != ApplicationModel.Database.Value.Name || currentSearch != SearchText.Value)
			{
				currentDatabase = ApplicationModel.Database.Value.Name;
				currentSearch = SearchText.Value;
				GroupedTransformers.Clear();
				return;
			}

			foreach (var groupedTransformer in GroupedTransformers)
			{
				groupedTransformer.Items.Clear();
			}
		}

		public Group SelectedGroup
		{
			get { return selectedGroup; }
			set
			{
				if (value == null)
					return;
				selectedGroup = value;
			}
		}

		public ICommand CollapseAll
		{
			get
			{
				return new ActionCommand(() =>
				{
					foreach (var groupedTransformer in GroupedTransformers)
					{
						groupedTransformer.Collapse.Value = true;
					}
				});
			}
		}

		public ICommand ExpandAll
		{
			get
			{
				return new ActionCommand(() =>
				{
					foreach (var groupedTransformer in GroupedTransformers)
					{
						groupedTransformer.Collapse.Value = false;
					}
				});
			}
		}

		public Observable<string> SearchText { get; set; }
		public ICommand ToggleGrouping
		{
			get
			{
				return new ActionCommand(() =>
				{
					UseGrouping.Value = !UseGrouping.Value;
					GroupedTransformers.Clear();
					TimerTickedAsync();
				});
			}
		}

		public ICommand DeleteGroupTransformers
		{
			get { return new DeleteTransformersCommand(this);}
		}

		private class DeleteTransformersCommand : Command
		{
			private readonly TransformersModel model;

			public DeleteTransformersCommand(TransformersModel model)
			{
				this.model = model;
			}

			public override void Execute(object parameter)
			{
				AskUser.ConfirmationAsync("Confirm Delete",
					"Really delete all transformers in group: '" + model.SelectedGroup.GroupName + "?")
					.ContinueWhenTrue(DeleteTransformers);
			}

			private void DeleteTransformers()
			{
				var tasks = (from transformer in model.SelectedGroup.Items.Select(item => item.Name)
							 select new { Task = DatabaseCommands.DeleteTransformerAsync(transformer), Name = transformer }).ToArray();

				Task.Factory.ContinueWhenAll(tasks.Select(x => x.Task).ToArray(), taskslist =>
				{
					foreach (var task in taskslist)
					{
						var transformerName = tasks.First(x => x.Task == task).Name;
						if (task.IsFaulted)
						{
							ApplicationModel.Current.AddErrorNotification(task.Exception,
								"transformer " + transformerName + " could not be deleted");
						}
						else
						{
							ApplicationModel.Current.AddInfoNotification("Transformer " + transformerName + " successfully deleted");
							var deletedItem = model.Transformers.FirstOrDefault(item => item.Name == transformerName);
							model.Transformers.Remove(deletedItem);
						}
					}
				});
			}
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
				var name = model.ItemSelection.Name;
				AskUser.ConfirmationAsync("Confirm Delete", "Really delete '" + name + "' transformer?")
					.ContinueWhenTrue(() => DeleteTransformer(name));
			}

			private void DeleteTransformer(string name)
			{
				DatabaseCommands
					.DeleteTransformerAsync(name)
					.ContinueOnUIThread(t =>
					{
						if (t.IsFaulted)
						{
							ApplicationModel.Current.AddErrorNotification(t.Exception,
								"transformer " + name + " could not be deleted");
						}
						else
						{
							ApplicationModel.Current.AddInfoNotification("transformer " + name + " successfully deleted");
							UrlUtil.Navigate("/transformers");
						}
					});
			}
		}
	}
}