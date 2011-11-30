using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Input;
using Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection.Async;
using Raven.Studio.Commands;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;

namespace Raven.Studio.Models
{
	public class IndexDefinitionModel : ViewModel
	{
		private readonly Observable<DatabaseStatistics> statistics;
		private IndexDefinition index;
		private string originalIndex;
		private bool createNewIndexMode;

		public IndexDefinitionModel()
		{
			ModelUrl = "/indexes/";
			index = new IndexDefinition();
			Maps = new BindableCollection<MapItem>(x => x.Text);
			Maps.Add(new MapItem()); // We must have at least one map item in a new index.
			Fields = new BindableCollection<FieldProperties>(field => field.Name);

			statistics = Database.Value.Statistics;
			statistics.PropertyChanged += (sender, args) => OnPropertyChanged("ErrorsCount");
		}

		private void UpdateFromIndex(IndexDefinition indexDefinition)
		{
			index = indexDefinition;
			Maps.Set(index.Maps.Select(x => new MapItem {Text = x}));

			CreateOrEditField(index.Indexes, (f, i) => f.Indexing = i);
			CreateOrEditField(index.Stores, (f, i) => f.Storage = i);
			CreateOrEditField(index.SortOptions, (f, i) => f.Sort = i);
			CreateOrEditField(index.Analyzers, (f, i) => f.Analyzer = i);

			OnEverythingChanged();
		}

		public override void LoadModelParameters(string parameters)
		{
			var urlParser = new UrlParser(parameters);
			if (urlParser.GetQueryParam("mode") == "new")
			{
				createNewIndexMode = true;
				OnPropertyChanged("ViewTitle");
				return;
			}

			var name = urlParser.Path;
			if (string.IsNullOrWhiteSpace(name))
				UrlUtil.Navigate("/indexes");

			DatabaseCommands.GetIndexAsync(name)
				.ContinueOnSuccessInTheUIThread(index1 =>
				                   {
				                   	if (index1 == null)
				                   	{
										HandleIndexNotFound(name);
				                   		return;
				                   	}
									originalIndex = JsonConvert.SerializeObject(index);
									UpdateFromIndex(index1);
				                   }).Catch();
		}

		public static void HandleIndexNotFound(string name)
		{
			var notification = new Notification(string.Format("Could not find '{0}' index", name), NotificationLevel.Warning);
			ApplicationModel.Current.AddNotification(notification);
			UrlUtil.Navigate("/documents");
		}

		private void ResetToOriginal()
		{
			index = JsonConvert.DeserializeObject<IndexDefinition>(originalIndex);
			UpdateFromIndex(index);
		}

		private void UpdateIndex()
		{
			index.Map = Maps.Select(x => x.Text).FirstOrDefault();
			index.Maps = new HashSet<string>(Maps.Select(x => x.Text));
			UpdateFields();
		}

		private void UpdateFields()
		{
			index.Indexes.Clear();
			index.Stores.Clear();
			index.SortOptions.Clear();
			index.Analyzers.Clear();
			foreach (var item in Fields.Where(item => item.Name != null))
			{
				index.Indexes[item.Name] = item.Indexing;
				index.Stores[item.Name] = item.Storage;
				index.SortOptions[item.Name] = item.Sort;
				index.Analyzers[item.Name] = item.Analyzer;
			}
			index.RemoveDefaultValues();
		}

		void CreateOrEditField<T>(IEnumerable<KeyValuePair<string, T>> dictionary, Action<FieldProperties, T> setter)
		{
			if (dictionary == null) return;

			foreach (var item in dictionary)
			{
				var localItem = item;
				var field = Fields.FirstOrDefault(f => f.Name == localItem.Key);
				if (field == null)
				{
					field = new FieldProperties { Name = localItem.Key };
					Fields.Add(field);
				}
				setter(field, localItem.Value);
			}
		}	

		public string Name
		{
			get { return index.Name; }
			set
			{
				index.Name = value;
				OnPropertyChanged();
			}
		}

		public string ViewTitle
		{
			get { return createNewIndexMode ? "Create an Index" : "Index: " + Name; }
		}

		public string Reduce
		{
			get { return index.Reduce; }
			set
			{
				index.Reduce = value;
				OnPropertyChanged();
			}
		}

		public string TransformResults
		{
			get { return index.TransformResults; }
			set
			{
				index.TransformResults = value;
				OnPropertyChanged();
			}
		}

		public BindableCollection<MapItem> Maps { get; private set; }
		public BindableCollection<FieldProperties> Fields { get; private set; }

		public int ErrorsCount
		{
			get
			{
				var databaseStatistics = statistics.Value;
				return databaseStatistics == null ? 0 : databaseStatistics.Errors.Count();
			}
		}

#region Commands

		public ICommand AddMap
		{
			get { return new ChangeFieldValueCommand<IndexDefinitionModel>(this, x => x.Maps.Add(new MapItem())); }
		}

		public ICommand RemoveMap
		{
			get { return new RemoveMapCommand(this); }
		}

		public ICommand AddReduce
		{
			get { return new ChangeFieldValueCommand<IndexDefinitionModel>(this, x => x.Reduce = string.Empty); }
		}

		public ICommand RemoveReduce
		{
			get { return new ChangeFieldValueCommand<IndexDefinitionModel>(this, x => x.Reduce = null); }
		}

		public ICommand AddTransformResults
		{
			get { return new ChangeFieldValueCommand<IndexDefinitionModel>(this, x => x.TransformResults = string.Empty); }
		}

		public ICommand RemoveTransformResults
		{
			get { return new ChangeFieldValueCommand<IndexDefinitionModel>(this, x => x.TransformResults = null); }
		}

		public ICommand AddField
		{
			get { return new ChangeFieldValueCommand<IndexDefinitionModel>(this, x => x.Fields.Add(FieldProperties.Defualt)); }
		}

		public ICommand RemoveField
		{
			get { return new RemoveFieldCommand(this); }
		}

		public ICommand SaveIndex
		{
			get { return new SaveIndexCommand(this); }
		}

		public ICommand DeleteIndex
		{
			get { return new DeleteIndexCommand(this); }
		}

		public ICommand ResetIndex
		{
			get { return new ResetIndexCommand(this); }
		}

		private class RemoveMapCommand : Command
		{
			private readonly IndexDefinitionModel index;

			public RemoveMapCommand(IndexDefinitionModel index)
			{
				this.index = index;
			}

			public override void Execute(object parameter)
			{
				var map = parameter as MapItem;
				if (map == null || index.Maps.Contains(map) == false)
					return;

				index.Maps.Remove(map);
			}
		}

		private class RemoveFieldCommand : Command
		{
			private FieldProperties field;
			private readonly IndexDefinitionModel index;

			public RemoveFieldCommand(IndexDefinitionModel index)
			{
				this.index = index;
			}

			public override bool CanExecute(object parameter)
			{
				field = parameter as FieldProperties;
				return field != null && index.Fields.Contains(field);
			}

			public override void Execute(object parameter)
			{
				index.Fields.Remove(field);
			}
		}

		private class SaveIndexCommand : Command
		{
			private readonly IndexDefinitionModel index;

			public SaveIndexCommand(IndexDefinitionModel index)
			{
				this.index = index;
			}

			public override void Execute(object parameter)
			{
				index.UpdateIndex();
				ApplicationModel.Current.AddNotification(new Notification("saving index " + index.Name));
				DatabaseCommands.PutIndexAsync(index.Name, index.index, true)
					.ContinueOnSuccess(() => ApplicationModel.Current.AddNotification(new Notification("index " + index.Name + " saved")))
					.Catch();
			}
		}

		private class ResetIndexCommand : Command
		{
			private readonly IndexDefinitionModel index;

			public ResetIndexCommand(IndexDefinitionModel index)
			{
				this.index = index;
			}

			public override void Execute(object parameter)
			{
				ApplicationModel.Current.AddNotification(new Notification("resetting index " + index.Name));
				index.ResetToOriginal();
				ApplicationModel.Current.AddNotification(new Notification("index " + index.Name + " was reset"));
			}
		}

		private class DeleteIndexCommand : Command
		{
			private readonly IndexDefinitionModel index;

			public DeleteIndexCommand(IndexDefinitionModel index)
			{
				this.index = index;
			}

			public override bool CanExecute(object parameter)
			{
				return index != null && string.IsNullOrWhiteSpace(index.Name) == false;
			}

			public override void Execute(object parameter)
			{
				AskUser.ConfirmationAsync("Confirm Delete", "Really delete '" + index.Name + "' index?")
					.ContinueWhenTrue(DeleteIndex);
			}

			private void DeleteIndex()
			{
				DatabaseCommands
					.DeleteIndexAsync(index.Name)
					.ContinueOnSuccessInTheUIThread(() =>
					                                	{
					                                		ApplicationModel.Current.AddNotification(
					                                			new Notification("index " + index.Name + " successfully deleted"));
					                                		UrlUtil.Navigate("/indexes");
					                                	});
			}
		}

		#endregion Commands

		public class MapItem
		{
			public string Text { get; set; }
		}

		public class FieldProperties
		{
			public string Name { get; set; }
			public FieldStorage Storage { get; set; }
			public FieldIndexing Indexing { get; set; }
			public SortOptions Sort { get; set; }
			public string Analyzer { get; set; }

			public static FieldProperties Defualt
			{
				get
				{
					return new FieldProperties
					{
						Storage = FieldStorage.No,
						Indexing = FieldIndexing.Default,
						Sort = SortOptions.None,
						Analyzer = string.Empty
					};
				}
			}
		}
	}
}