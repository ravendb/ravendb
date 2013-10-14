using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows.Input;
using DrWPF.Windows.Data;
using Raven.Abstractions.Exceptions;
using Raven.Client.Connection;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Studio.Behaviors;
using Raven.Studio.Commands;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Infrastructure.Converters;
using Raven.Studio.Messages;
using Raven.Abstractions.Extensions;

namespace Raven.Studio.Models
{
	public class IndexDefinitionModel : PageViewModel, IHasPageTitle, IAutoCompleteSuggestionProvider
	{
        public const string CollectionsIndex = "Raven/DocumentsByEntityName";
		private readonly Observable<DatabaseStatistics> statistics;
		private IndexDefinition index;
		private string originalIndex;
		private bool isNewIndex;
		private bool hasUnsavedChanges;
		public string OriginalName { get; private set; }

		public bool PriorityChanged = false;
		public Observable<string> IndexingPriority { get; set; }
		public IndexingPriority Priority { get; set; }
		public List<string> Priorities { get; set; }

		public static string MapQuery { get; private set; }

        public ObservableCollection<IndexDefinitionError> Errors { get; private set; }
		public IndexDefinitionModel()
		{
			ModelUrl = "/indexes/";
			Priorities = new List<string> {"Normal", "Idle", "Disabled", "Abandoned"};
			ApplicationModel.Current.Server.Value.RawUrl = null;
			IndexingPriority = new Observable<string>();

			IndexingPriority.PropertyChanged += (sender, args) => PriorityChanged = true;
			index = new IndexDefinition();
			Maps = new BindableCollection<MapItem>(x => x.Text)
			{
				new MapItem()
			};



			Maps.CollectionChanged += HandleChildCollectionChanged;
			Maps[0].PropertyChanged += (sender, args) => { MapQuery = Maps[0].Text; };

			Fields = new BindableCollection<FieldProperties>(field => field.Name);
			Fields.CollectionChanged += HandleChildCollectionChanged;

			SpatialFields = new BindableCollection<SpatialFieldProperties>(field => field.Name);
			SpatialFields.CollectionChanged += HandleChildCollectionChanged;

			statistics = Database.Value.Statistics;
			statistics.PropertyChanged += (sender, args) => UpdateErrors();

            propertyHasError = new ObservableDictionary<string, bool>();
		    propertyHasError["TransformResults"] = false;
		    propertyHasError["Name"] = false;
		    propertyHasError["Reduce"] = false;

            Errors = new ObservableCollection<IndexDefinitionError>();
		}

	    private void UpdateErrors()
	    {
	        bool hadIndexingErrors = Errors.Any(e => e.Stage == "Indexing");

	        var serverErrors = Database.Value.Statistics.Value.Errors.Where(s => s.Index == index.IndexId).Select(se => new IndexDefinitionError()
	        {
	            DocumentId = se.Document,
                Message = se.Error,
                Section = se.Action,
                Stage = "Indexing"
	        }).ToList();

            if (hadIndexingErrors || serverErrors.Any())
            {
                ClearErrorsForStage("Indexing");
            }

            Errors.AddRange(serverErrors);

            if (serverErrors.Any(se => se.Section == "Map"))
            {
                foreach (var mapItem in Maps)
                {
                    mapItem.HasError = true;
                }
            }
            

            if (serverErrors.Any(se => se.Section == "Reduce"))
            {
                propertyHasError["Reduce"] = true;
            }

	        IsShowingErrors = Errors.Any();
	    }

	    private void ClearErrorsForStage(string stage)
	    {
	        for (int i = Errors.Count - 1; i >= 0; i--)
	        {
	            if (Errors[i].Stage == stage)
	            {
	                Errors.RemoveAt(i);
	            }
	        }
	    }

	    private void HandleChildCollectionChanged(object sender, NotifyCollectionChangedEventArgs e)
		{
			MarkAsDirty();

		    if (e.Action == NotifyCollectionChangedAction.Add)
			{
				var newItem = e.NewItems[0] as INotifyPropertyChanged;
				if (newItem != null)
					newItem.PropertyChanged += HandleChildItemChanged;
			}
		}

		private void HandleChildItemChanged(object sender, PropertyChangedEventArgs e)
		{
			MarkAsDirty();
		}

		private void UpdateFromIndex(IndexDefinition indexDefinition)
		{
			UpdatePriority(indexDefinition.IndexId);
			index = indexDefinition;

			if (index.Maps.Count == 0)
				index.Maps.Add("");

			Maps.Set(index.Maps.Select(x => new MapItem { Text = x }));
			Maps[0].PropertyChanged += (sender, args) => { MapQuery = Maps[0].Text; };
			MapQuery = Maps[0].Text;
			ShowReduce = Reduce != null;
			ShowTransformResults = TransformResults != null;

			CreateOrEditField(index.Indexes, (f, i) => f.Indexing = i);
			CreateOrEditField(index.Stores, (f, i) => f.Storage = i);
			CreateOrEditField(index.TermVectors, (f, i) => f.TermVector = i);
			CreateOrEditField(index.SortOptions, (f, i) => f.Sort = i);
			CreateOrEditField(index.Analyzers, (f, i) => f.Analyzer = i);
			CreateOrEditField(index.Suggestions, (f, i) =>
			{
				f.SuggestionAccuracy = i.Accuracy;
				f.SuggestionDistance = i.Distance;
			});

			foreach (var pair in index.SpatialIndexes)
			{
				var field = SpatialFields.FirstOrDefault(f => f.Name == pair.Key);
				if (field == null)
					SpatialFields.Add(new SpatialFieldProperties(pair));
				else
					field.UpdateFromSpatialOptions(pair.Value);
			}


			RestoreDefaults(index);

            UpdateErrors();

			hasUnsavedChanges = false;

			OnEverythingChanged();
		}

		private void UpdatePriority(int id)
		{
			DatabaseCommands
				.GetStatisticsAsync()
				.ContinueOnSuccessInTheUIThread(databaseStatistics =>
				{
					var indexStats = databaseStatistics.Indexes.FirstOrDefault(stats => stats.Id == id);
					if (indexStats == null)
						return;
					Priority = indexStats.Priority;
					if(Priority == 0)
						Priority = Abstractions.Data.IndexingPriority.Normal;
					if (Priority.HasFlag(Abstractions.Data.IndexingPriority.Normal))
						IndexingPriority.Value = "Normal";
					else if (Priority.HasFlag(Abstractions.Data.IndexingPriority.Idle))
						IndexingPriority.Value = "Idle";
					else if (Priority.HasFlag(Abstractions.Data.IndexingPriority.Disabled))
						IndexingPriority.Value = "Disabled";
					else if (Priority.HasFlag(Abstractions.Data.IndexingPriority.Abandoned))
						IndexingPriority.Value = "Abandoned";

					PriorityChanged = false;
				});
		}

		private void RestoreDefaults(IndexDefinition indexDefinition)
		{
			foreach (var field in Fields)
			{
				if(indexDefinition.Stores.ContainsKey(field.Name) == false)
					field.Storage = FieldStorage.No;
			}
		}

		public override void LoadModelParameters(string parameters)
		{
			var urlParser = new UrlParser(parameters);
			if (urlParser.GetQueryParam("mode") == "new")
			{
				IsNewIndex = true;
				Header = "New Index";

				UpdateFromIndex(new IndexDefinition());

				return;
			}

			var name = urlParser.Path;
			if (string.IsNullOrWhiteSpace(name))
				HandleIndexNotFound(null);

			Header = name;
			OriginalName = name;
			IsNewIndex = false;

            ClearDefinitionErrors();

			DatabaseCommands.GetIndexAsync(name)
				.ContinueOnUIThread(task =>
										{
											if (task.IsFaulted || task.Result == null)
											{
												HandleIndexNotFound(name);
												return;
											}
											originalIndex = JsonConvert.SerializeObject(task.Result);
											UpdateFromIndex(task.Result);
										}).Catch();
		}

		public override bool CanLeavePage()
		{
		    return !hasUnsavedChanges || AskUser.Confirmation("Edit Index",
		                                                      "There are unsaved changes to this index. Are you sure you want to continue?");
		}

        public bool IsShowingErrors
        {
            get { return isShowingErrors; }
            set
            {
                isShowingErrors = value;
                OnPropertyChanged(() => IsShowingErrors);
            }
        }

	    public static void HandleIndexNotFound(string name)
		{
			if (string.IsNullOrWhiteSpace(name) == false)
			{
				var notification = new Notification(string.Format("Could not find index '{0}'", name), NotificationLevel.Warning);
				ApplicationModel.Current.AddNotification(notification);
			}
			UrlUtil.Navigate("/indexes");
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
			index.Suggestions.Clear();
			index.TermVectors.Clear();
			index.SpatialIndexes.Clear();
			foreach (var item in Fields.Where(item => item.Name != null))
			{
				index.Indexes[item.Name] = item.Indexing;
				index.Stores[item.Name] = item.Storage;
				index.SortOptions[item.Name] = item.Sort;
				index.Analyzers[item.Name] = item.Analyzer;
				index.TermVectors[item.Name] = item.TermVector;
				index.Suggestions[item.Name] = new SuggestionOptions { Accuracy = item.SuggestionAccuracy, Distance = item.SuggestionDistance };
			}
			foreach (var item in SpatialFields.Where(item => item.Name != null))
			{
				index.SpatialIndexes[item.Name] = new SpatialOptions
				                                  {
					                                  Type = item.Type,
													  Strategy = item.Strategy,
													  MaxTreeLevel = item.MaxTreeLevel,
													  MinX = item.MinX,
													  MaxX = item.MaxX,
													  MinY = item.MinY,
													  MaxY = item.MaxY,
													  Units = item.Units
				                                  };
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
					field = FieldProperties.Default;
					field.Name = localItem.Key;
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
				if (index.Name != value)
				{
					MarkAsDirtyIfSignificant(index.Name, value);
					index.Name = value;
					OnPropertyChanged(() => Name);
				}
			}
		}

		private void MarkAsDirtyIfSignificant(string oldValue, string newValue)
		{
			if (!(string.IsNullOrEmpty(oldValue) && string.IsNullOrEmpty(newValue)))
				MarkAsDirty();
		}

		//public string MapUrl
		//{
		//    get{return }
		//}

		private string header;
		public string Header
		{
			get { return header; }
			set
			{
				header = value;
				OnPropertyChanged(() => Header);
			}
		}

		private bool showReduce;
		public bool ShowReduce
		{
			get { return showReduce; }
			set
			{
				showReduce = value;
				OnPropertyChanged(() => ShowReduce);
			}
		}

		public string Reduce
		{
			get { return index.Reduce; }
			set
			{
				if (index.Reduce != value)
				{
					MarkAsDirtyIfSignificant(index.Reduce, value);
					index.Reduce = value;
					OnPropertyChanged(() => Reduce);
					OnPropertyChanged(() => ReduceHeight);
				}
			}
		}

		public double ReduceHeight
		{
			get
			{
				return TextHeight(Reduce);
			}
		}

		private double TextHeight(string text)
		{
			if (text == null)
				return 100;
			var len = text.Count(ch => ch == '\n');
			if (len < 4)
				return 100;
			if (len < 8)
				return 180;
			if (len < 12)
				return 230;
			return 300;
		}

		private void MarkAsDirty()
		{
			hasUnsavedChanges = true;
		}

		private bool showTransformResults;
	    private string definitionErrorMessage;
	    private ObservableDictionary<string, bool> propertyHasError;
	    private bool isShowingErrors;
	    private ICommand showSampleDocument;

	    public bool ShowTransformResults
		{
			get { return showTransformResults; }
			set
			{
				showTransformResults = value;
				OnPropertyChanged(() => ShowTransformResults);
			}
		}

		public string TransformResults
		{
			get { return index.TransformResults; }
			set
			{
				if (index.TransformResults != value)
				{
					MarkAsDirtyIfSignificant(index.TransformResults, value);
					index.TransformResults = value;
					OnPropertyChanged(() => TransformResults);
					OnPropertyChanged(() => TransformHeight);
				}
			}
		}

		public double TransformHeight
		{
			get
			{
				return TextHeight(TransformResults);
			}
		}

		public BindableCollection<MapItem> Maps { get; private set; }
		public BindableCollection<FieldProperties> Fields { get; private set; }
		public BindableCollection<SpatialFieldProperties> SpatialFields { get; private set; }

	    public ICommand ShowSampleDocument
	    {
	        get { return showSampleDocument ?? (showSampleDocument = new AsyncActionCommand(HandleShowSampleDocument)); }
	    }

	    private async Task HandleShowSampleDocument(object parameter)
	    {
	        var mapItem = parameter as MapItem;
            if (mapItem == null || string.IsNullOrWhiteSpace(mapItem.Text))
            {
                ApplicationModel.Current.ShowDocumentInDocumentPad("");
                return;
            }

	        const string collectionNameRegEx = @"docs\.(?<collection>[\w]+)";
	        var match = Regex.Match(mapItem.Text, collectionNameRegEx);
            if (!match.Success)
            {
                ApplicationModel.Current.ShowDocumentInDocumentPad("");
                return;
            }

	        var collectionName = match.Groups[1].ToString();
	        var results =
	            await
	            DatabaseCommands.QueryAsync(CollectionsIndex,
	                                        new IndexQuery() {Query = "Tag:" + collectionName, PageSize = 1}, null,
	                                        metadataOnly: true);

            if (results.Results.Count == 0)
            {
                ApplicationModel.Current.ShowDocumentInDocumentPad("");
                return;
            }

	        var docId = results.Results[0].SelectToken("@metadata.@id").ToString();
            ApplicationModel.Current.ShowDocumentInDocumentPad(docId);
	    }

	    public int ErrorsCount
		{
			get
			{
				var databaseStatistics = statistics.Value;
				return databaseStatistics == null ? 0 : databaseStatistics.Errors.Count(e => e.Index == index.IndexId);
			}
		}

	    public string DefinitionErrorMessage
	    {
            get { return definitionErrorMessage; }
	        private set
	        {
	            definitionErrorMessage = value;
	            OnPropertyChanged(() => DefinitionErrorMessage);
	        }
	    }

        private void ClearDefinitionErrors()
        {
            ClearErrorsForStage("Compilation");

            foreach (var mapItem in Maps)
            {
                mapItem.HasError = false;
            }


            ClearPropertyErrors();
        }

        private void ReportDefinitionError(string message, string property, string problematicText = "")
        {
           Errors.Add(new IndexDefinitionError()
           {
               Message = message,
               Section = property,
               Stage = "Compilation",
           });

            if (property == "Maps")
            {
                foreach (var mapItem in Maps)
                {
                    if (mapItem.Text.Equals(problematicText, StringComparison.Ordinal))
                    {
                        mapItem.HasError = true;
                    }
                }
            }
            else
            {
                PropertyHasError[property] = true;
            }

            IsShowingErrors = true;
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
			get { return new ChangeFieldValueCommand<IndexDefinitionModel>(this, x => x.ShowReduce = true); }
		}

		public ICommand RemoveReduce
		{
			get { return new ChangeFieldValueCommand<IndexDefinitionModel>(this, x => x.ShowReduce = false); }
		}

		public ICommand AddTransformResults
		{
			get { return new ChangeFieldValueCommand<IndexDefinitionModel>(this, x => x.ShowTransformResults = true); }
		}

		public ICommand RemoveTransformResults
		{
			get { return new ChangeFieldValueCommand<IndexDefinitionModel>(this, x => x.ShowTransformResults = false); }
		}

		public ICommand AddField
		{
			get { return new ChangeFieldValueCommand<IndexDefinitionModel>(this, x => x.Fields.Add(FieldProperties.Default)); }
		}

		public ICommand AddSpatialField
		{
			get { return new ChangeFieldValueCommand<IndexDefinitionModel>(this, x => x.SpatialFields.Add(SpatialFieldProperties.Default)); }
		}

		public ICommand RemoveField
		{
			get { return new RemoveFieldCommand(this); }
		}

		public ICommand RemoveSpatialField
		{
			get { return new RemoveSpatialFieldCommand(this); }
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

		private class RemoveSpatialFieldCommand : Command
		{
			private SpatialFieldProperties field;
			private readonly IndexDefinitionModel index;

			public RemoveSpatialFieldCommand(IndexDefinitionModel index)
			{
				this.index = index;
			}

			public override bool CanExecute(object parameter)
			{
				field = parameter as SpatialFieldProperties;
				return field != null && index.SpatialFields.Contains(field);
			}

			public override void Execute(object parameter)
			{
				index.SpatialFields.Remove(field);
			}
		}

		private class SaveIndexCommand : Command
		{
			private readonly IndexDefinitionModel index;

			public SaveIndexCommand(IndexDefinitionModel index)
			{
				this.index = index;
			}

			public override bool CanExecute(object parameter)
			{
				return index.index.LockMode == IndexLockMode.Unlock;
			}

			public override void Execute(object parameter)
			{
                index.ClearDefinitionErrors();

				if (string.IsNullOrWhiteSpace(index.Name))
				{
					index.ReportDefinitionError("Index must have a name!", "Name");
					return;
				}

				if (index.Maps.All(item => string.IsNullOrWhiteSpace(item.Text)))
				{
					index.ReportDefinitionError("Index must have at least one map with data!", "Map");
					return;
				}

				if (index.IsNewIndex == false && index.OriginalName != index.Name)
				{
					if (AskUser.Confirmation("Indexes cannot be renamed",
						                     "If you continue, a new index will be created with this name.") ==false)
					{
						ApplicationModel.Current.Notifications.Add(new Notification("Index Not Saved"));
						return;
					}
				}

				index.UpdateIndex();
				if (index.Reduce == "")
					index.Reduce = null;
				if (index.TransformResults == "" || index.ShowTransformResults == false)
					index.TransformResults = null;

				var mapIndexes = (from mapItem in index.Maps where mapItem.Text == "" select index.Maps.IndexOf(mapItem)).ToList();
				mapIndexes.Sort();

				for (int i = mapIndexes.Count - 1; i >= 0; i--)
				{
					index.Maps.RemoveAt(mapIndexes[i]);
				}

				SavePriority(index);

				ApplicationModel.Current.AddNotification(new Notification("Saving index " + index.Name));
				DatabaseCommands.PutIndexAsync(index.Name, index.index, true)
					.ContinueOnSuccess(() =>
										   {
											   ApplicationModel.Current.AddNotification(
												   new Notification("Index " + index.Name + " saved"));
											   index.hasUnsavedChanges = false;
											   PutIndexNameInUrl(index.Name);
										   })
					.Catch(ex =>
					{
                        var indexException = ex.ExtractSingleInnerException() as IndexCompilationException;
                        if (indexException != null)
                        {
                            index.ReportDefinitionError(indexException.Message, indexException.IndexDefinitionProperty, indexException.ProblematicText);
                            return true;
                        }
					    return false;
					});
			}

			private void SavePriority(IndexDefinitionModel indexDefinitionModel)
			{
				if (indexDefinitionModel.PriorityChanged == false)
					return;

				var priority = Abstractions.Data.IndexingPriority.Normal;
				switch (indexDefinitionModel.IndexingPriority.Value)
				{
					case "Normal":
						priority = Abstractions.Data.IndexingPriority.Normal;
						break;
					case "Idle":
						priority = Abstractions.Data.IndexingPriority.Idle | Abstractions.Data.IndexingPriority.Forced;
						break;
					case "Disabled":
						priority = Abstractions.Data.IndexingPriority.Disabled | Abstractions.Data.IndexingPriority.Forced;
						break;
					case "Abandoned":
						priority = Abstractions.Data.IndexingPriority.Abandoned | Abstractions.Data.IndexingPriority.Forced;
						break;
				}
				var priorityString = priority.ToString().Replace(" ", "");
				ApplicationModel.Current.Server.Value.SelectedDatabase.Value
								.AsyncDatabaseCommands
								.CreateRequest(string.Format("/indexes/set-priority/{0}?priority={1}", Uri.EscapeUriString(indexDefinitionModel.Name), priorityString), "POST")
				                .ExecuteRequestAsync();
			}

			private void PutIndexNameInUrl(string name)
			{
				if (index.IsNewIndex || index.Header != name)
					UrlUtil.Navigate("/indexes/" + name, true);
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
				ApplicationModel.Current.AddNotification(new Notification("Resetting index " + index.Name));
				index.ResetToOriginal();
				ApplicationModel.Current.AddNotification(new Notification("Index " + index.Name + " was reset"));
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
				return index != null && index.IsNewIndex == false;
			}

			public override void Execute(object parameter)
			{
				AskUser.ConfirmationAsync("Confirm Delete", "Are you sure you want to delete index '" + index.Name + "'?")
					.ContinueWhenTrue(DeleteIndex);
			}

			private void DeleteIndex()
			{
				DatabaseCommands
					.DeleteIndexAsync(index.Name)
					.ContinueOnUIThread(t =>
														{
															if (t.IsFaulted)
															{
																ApplicationModel.Current.AddErrorNotification(t.Exception, "Index " + index.Name + " could not be deleted");
															}
															else
															{
																ApplicationModel.Current.AddInfoNotification("Index '" + index.Name + "' successfully deleted");
																UrlUtil.Navigate("/indexes");
															}
														});
			}
		}

		#endregion Commands

		public class MapItem : NotifyPropertyChangedBase
		{
			public MapItem()
			{
				text = string.Empty;
			}
			private string text;
		    private bool hasError;

		    public string Text
			{
				get { return text; }
				set
				{
					if (text != value)
					{
						text = value;
						OnPropertyChanged(() => Text);
						OnPropertyChanged(() => TextHeight);
					}
				}
			}

		    public bool HasError
		    {
		        get { return hasError; }
		        set
		        {
		            hasError = value;
		            OnPropertyChanged(() => HasError);
		        }
		    }

			public double TextHeight
			{
				get
				{
					var len = text.Count(ch => ch == '\n');
					if (len < 4)
						return 100;
					if (len < 8)
						return 180;
					if (len < 12)
						return 230;
					return 300;
				}
			}
		}

		public class SpatialFieldProperties : NotifyPropertyChangedBase
		{
			private string name;
			public string Name
			{
				get { return name; }
				set
				{
					if (name != value)
					{
						name = value;
						OnPropertyChanged(() => Name);
					}
				}
			}

			private SpatialFieldType type;
			public SpatialFieldType Type
			{
				get { return type; }
				set
				{
					if (type != value)
					{
						type = value;
						OnPropertyChanged(() => Type);
						ResetToDefaults();
						UpdatePrecision();
					}

					IsGeographical = value == SpatialFieldType.Geography;
					IsCartesian = value == SpatialFieldType.Cartesian;
				}
			}

			private SpatialSearchStrategy strategy;
			public SpatialSearchStrategy Strategy
			{
				get { return strategy; }
				set
				{
					if (strategy != value)
					{
						strategy = value;
						OnPropertyChanged(() => Strategy);

						if (type == SpatialFieldType.Geography)
						{
							if (strategy == SpatialSearchStrategy.GeohashPrefixTree)
								MaxTreeLevel = SpatialOptions.DefaultGeohashLevel;
							if (strategy == SpatialSearchStrategy.QuadPrefixTree)
								MaxTreeLevel = SpatialOptions.DefaultQuadTreeLevel;
						}

						UpdatePrecision();
					}

					if (strategy == SpatialSearchStrategy.BoundingBox)
					{
						MaxTreeLevel = 0;
						IsPrefixTreeIndex = false;
					}
					else
					{
						IsPrefixTreeIndex = true;
					}
				}
			}

			private int maxTreeLevel;
			public int MaxTreeLevel
			{
				get { return maxTreeLevel; }
				set
				{
					if (maxTreeLevel != value)
					{
						maxTreeLevel = value;
						OnPropertyChanged(() => MaxTreeLevel);
						UpdatePrecision();
					}
				}
			}

			private double minX;
			public double MinX
			{
				get { return minX; }
				set
				{
					if (minX != value)
					{
						minX = value;
						OnPropertyChanged(() => MinX);
						UpdatePrecision();
					}
				}
			}

			private double maxX;
			public double MaxX
			{
				get { return maxX; }
				set
				{
					if (maxX != value)
					{
						maxX = value;
						OnPropertyChanged(() => MaxX);
						UpdatePrecision();
					}
				}
			}

			private double minY;
			public double MinY
			{
				get { return minY; }
				set
				{
					if (minY != value)
					{
						minY = value;
						OnPropertyChanged(() => MinY);
						UpdatePrecision();
					}
				}
			}

			private double maxY;
			public double MaxY
			{
				get { return maxY; }
				set
				{
					if (maxY != value)
					{
						maxY = value;
						OnPropertyChanged(() => MaxY);
						UpdatePrecision();
					}
				}
			}

			private string precision;
			public string Precision
			{
				get { return precision; }
				set
				{
					if (precision != value)
					{
						precision = value;
						OnPropertyChanged(() => Precision);
					}
				}
			}

			private SpatialUnits units;
			public SpatialUnits Units
			{
				get { return units; }
				set
				{
					if (units != value)
					{
						units = value;
						OnPropertyChanged(() => Units);
						UpdatePrecision();
					}
				}
			}

			private bool isGeographical;
			public bool IsGeographical
			{
				get { return isGeographical; }
				set
				{
					if (isGeographical == value) return;
					isGeographical = value;
					OnPropertyChanged(() => IsGeographical);
				}
			}

			private bool isCartesian;
			public bool IsCartesian
			{
				get { return isCartesian; }
				set
				{
					if (isCartesian == value) return;
					isCartesian = value;
					OnPropertyChanged(() => IsCartesian);
				}
			}

			private bool isPrefixTreeIndex;
			public bool IsPrefixTreeIndex
			{
				get { return isPrefixTreeIndex; }
				set
				{
					if (isPrefixTreeIndex == value) return;
					isPrefixTreeIndex = value;
					OnPropertyChanged(() => IsPrefixTreeIndex);
				}
			}

			public List<object> CartesianStrategies
			{
				get
				{
					return typeof(SpatialSearchStrategy).GetFields()
						.Where(field => field.IsLiteral)
						.Select(field => field.GetValue(Strategy))
						.Cast<SpatialSearchStrategy>()
						.Where(field => field != SpatialSearchStrategy.GeohashPrefixTree)
						.Cast<object>()
						.ToList();
				}
			}

			public List<object> GeographyStrategies
			{
				get
				{
					return typeof(SpatialSearchStrategy).GetFields()
						.Where(field => field.IsLiteral)
						.Select(field => field.GetValue(Strategy))
						.ToList();
				}
			}

			private void ResetToDefaults()
			{
				if (type == SpatialFieldType.Geography)
				{
					Strategy = SpatialSearchStrategy.GeohashPrefixTree;
					MaxTreeLevel = SpatialOptions.DefaultGeohashLevel;
					Units = SpatialUnits.Kilometers;
					MinX = -180;
					MinY = -90;
					MaxX = 180;
					MaxY = 90;
				}

				if (type == SpatialFieldType.Cartesian)
				{
					Strategy = SpatialSearchStrategy.QuadPrefixTree;
					MaxTreeLevel = SpatialOptions.DefaultQuadTreeLevel;
				}
			}

			public SpatialFieldProperties() : base()
			{
				Type = SpatialFieldType.Geography;
				ResetToDefaults();
			}

			public SpatialFieldProperties(KeyValuePair<string, SpatialOptions> spatialOptions) : base()
			{
				Name = spatialOptions.Key;
				UpdateFromSpatialOptions(spatialOptions.Value);
			}

			public void UpdateFromSpatialOptions(SpatialOptions spatialOptions)
			{
				Type = spatialOptions.Type;
				ResetToDefaults();
				Strategy = spatialOptions.Strategy;
				MaxTreeLevel = spatialOptions.MaxTreeLevel;
				if (spatialOptions.Type == SpatialFieldType.Geography)
				{
					Units = spatialOptions.Units;
				}
				else
				{
					MinX = spatialOptions.MinX;
					MaxX = spatialOptions.MaxX;
					MinY = spatialOptions.MinY;
					MaxY = spatialOptions.MaxY;
				}
			}

			public static SpatialFieldProperties Default
			{
				get { return new SpatialFieldProperties(); }
			}

			public void UpdatePrecision()
			{
				if (strategy == SpatialSearchStrategy.BoundingBox)
				{
					Precision = string.Empty;
					return;
				}

				var x = maxX - minX;
				var y = maxY - minY;
				for (var i = 0; i < maxTreeLevel; i++)
				{
					if (strategy == SpatialSearchStrategy.GeohashPrefixTree)
					{
						if (i%2 == 0)
						{
							x /= 8;
							y /= 4;
						}
						else
						{
							x /= 4;
							y /= 8;
						}
					}
					else if (strategy == SpatialSearchStrategy.QuadPrefixTree)
					{
						x /= 2;
						y /= 2;
					}
				}

				if (type == SpatialFieldType.Geography)
				{
					const double factor = (Constants.EarthMeanRadiusKm*Math.PI*2)/360;
					x = x * factor;
					y = y * factor;
					if (units == SpatialUnits.Miles)
					{
						x /= Constants.MilesToKm;
						y /= Constants.MilesToKm;
					}
					Precision = string.Format(CultureInfo.InvariantCulture, "Precision at equator; X: {0:F6}, Y: {1:F6} {2}", x, y, units.ToString().ToLowerInvariant());
				}
				else
				{
					Precision = string.Format(CultureInfo.InvariantCulture, "Precision; X: {0:F6}, Y: {1:F6}", x, y);
				}
			}
		}

		public string PageTitle
		{
			get { return "Edit Index"; }
		}

		public bool IsNewIndex
		{
			get { return isNewIndex; }
			set
			{
				isNewIndex = value;
				OnPropertyChanged(() => IsNewIndex);
			}
		}

		public bool IsLocked
		{
			get { return index.LockMode != IndexLockMode.Unlock; }
		}

		public Task<IList<object>> ProvideSuggestions(string enteredText)
		{
			var list = new List<object>
			{
				"Raven.Database.Indexing.LowerCaseKeywordAnalyzer, Raven.Database",	
				"Raven.Database.Indexing.LowerCaseWhitespaceAnalyzer, Raven.Database",
				"Lucene.Net.Analysis.Standard.StandardAnalyzer, Lucene.Net",
				"Lucene.Net.Analysis.WhitespaceAnalyzer, Lucene.Net",
				"Lucene.Net.Analysis.StopAnalyzer, Lucene.Net",
				"Lucene.Net.Analysis.SimpleAnalyzer, Lucene.Net",
			};
			return TaskEx.FromResult<IList<object>>(list);
		}

	    public ObservableDictionary<string, bool> PropertyHasError
	    {
	        get { return propertyHasError; }
	    }

        protected void ClearPropertyErrors()
        {
            foreach (var property in propertyHasError.Keys)
            {
                PropertyHasError[property] = false;
            }
        }
	}

    public class IndexDefinitionError
    {
        public string Stage { get; set; }

        public string Section { get; set; }

        public string Message { get; set; }

        public string DocumentId { get; set; }
    }

	public class FieldProperties : NotifyPropertyChangedBase, IAutoCompleteSuggestionProvider
	{
		static private List<string> AvailableFields { get; set; }

		public FieldProperties()
		{
			AvailableFields = new List<string>();
		}
		static private string LastMap { get; set; }
		private string name;
		public string Name
		{
			get { return name; }
			set
			{
				if (name != value)
				{
					name = value;
					OnPropertyChanged(() => Name);
				}
			}
		}

		private FieldStorage storage;
		public FieldStorage Storage
		{
			get { return storage; }
			set
			{
				if (storage != value)
				{
					storage = value;
					OnPropertyChanged(() => Storage);
				}
			}
		}

		private FieldIndexing indexing;
		public FieldIndexing Indexing
		{
			get { return indexing; }
			set
			{
				if (indexing != value)
				{
					indexing = value;
					OnPropertyChanged(() => Indexing);
				}
			}
		}

		private FieldTermVector termVector;
		public FieldTermVector TermVector
		{
			get { return termVector; }
			set
			{
				if (termVector != value)
				{
					termVector = value;
					OnPropertyChanged(() => TermVector);
				}
			}
		}


		private SortOptions sort;
		public SortOptions Sort
		{
			get { return sort; }
			set
			{
				if (sort != value)
				{
					sort = value;
					OnPropertyChanged(() => Sort);
				}
			}
		}

		private string analyzer;
		public string Analyzer
		{
			get { return analyzer; }
			set
			{
				if (analyzer != value)
				{
					analyzer = value;
					OnPropertyChanged(() => Analyzer);
				}
			}
		}

		public static FieldProperties Default
		{
			get
			{
				return new FieldProperties
				{
					Storage = FieldStorage.No,
					Indexing = FieldIndexing.Default,
					TermVector = FieldTermVector.No,
					Sort = SortOptions.None,
					Analyzer = string.Empty,
					SuggestionAccuracy = 0.5f,
					SuggestionDistance = StringDistanceTypes.None,
				};
			}
		}

		private float suggestionAccuracy;
		public float SuggestionAccuracy
		{
			get { return suggestionAccuracy; }
			set
			{
				if (suggestionAccuracy != value)
				{
					suggestionAccuracy = value;
					OnPropertyChanged(() => suggestionAccuracy);
				}
			}
		}

		private StringDistanceTypes suggestionDistance;
		public StringDistanceTypes SuggestionDistance
		{
			get { return suggestionDistance; }
			set
			{
				if (suggestionDistance != value)
				{
					suggestionDistance = value;
					OnPropertyChanged(() => suggestionDistance);
				}
			}
		}

		public async Task<IList<object>> ProvideSuggestions(string enteredText)
		{
			if (LastMap != IndexDefinitionModel.MapQuery)
			{
				LastMap = IndexDefinitionModel.MapQuery;
				var request = ApplicationModel.Current.Server.Value.SelectedDatabase.Value
							.AsyncDatabaseCommands
							.CreateRequest(string.Format("/debug/index-fields").NoCache(), "POST");
				await request.WriteAsync(LastMap);
			var item = await request.ReadResponseJsonAsync();
				if (item != null)
				{
					AvailableFields = item.SelectToken("FieldNames").Values().Select(token => token.ToString()).ToList();
				}
			}

			return AvailableFields.Cast<object>().ToList();
		}
	}
}
