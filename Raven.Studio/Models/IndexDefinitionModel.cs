using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Windows.Input;
using Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class IndexDefinitionModel : Model
	{
		private IAsyncDatabaseCommands asyncDatabaseCommands;
		private IndexDefinition index;
		private string name;
		private string reduce;
		private string transformResults;

		public IndexDefinitionModel(IndexDefinition index, IAsyncDatabaseCommands asyncDatabaseCommands)
		{
			this.asyncDatabaseCommands = asyncDatabaseCommands;
			UpdateFromDocument(index);
		}

		private void UpdateFromDocument(IndexDefinition indexDefinition)
		{
			this.index = indexDefinition;
			this.name = index.Name;
			this.Maps = new ObservableCollection<MapItem>(index.Maps.Select(x => new MapItem{Text = x}));
			this.reduce = index.Reduce;
			this.transformResults = index.TransformResults;

			this.Fields = new ObservableCollection<FieldProperties>(index.Fields.Select(x => new FieldProperties { Name = x }));
			CreateOrEditField(index.Indexes, (f, i) => f.Indexing = i);
			CreateOrEditField(index.Stores, (f, i) => f.Storage = i);
			CreateOrEditField(index.SortOptions, (f, i) => f.Sort = i);
			CreateOrEditField(index.Analyzers, (f, i) => f.Analyzer = i);

			OnEverythingChanged();
		}

		void CreateOrEditField<T>(IDictionary<string, T> dictionary, Action<FieldProperties, T> setter)
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
			get { return name; }
			set
			{
				name = value;
				OnPropertyChanged();
			}
		}

		public string Reduce
		{
			get { return reduce; }
			set
			{
				reduce = value;
				OnPropertyChanged();
			}
		}

		public string TransformResults
		{
			get { return transformResults; }
			set
			{
				transformResults = value;
				OnPropertyChanged();
			}
		}

		public ObservableCollection<MapItem> Maps { get; private set; }
		public ObservableCollection<FieldProperties> Fields { get; private set; }

#region Commands

		public ICommand AddMap
		{
			get { return new AddMapCommand(this); }
		}

		public ICommand RemoveMap
		{
			get { return new RemoveMapCommand(this); }
		}

		public ICommand AddReduce
		{
			get { return new AddReduceCommand(this); }
		}

		public ICommand RemoveReduce
		{
			get { return new RemoveReduceCommand(this); }
		}

		public ICommand AddTransformResults
		{
			get { return new AddTransformResultsCommand(this); }
		}

		public ICommand RemoveTransformResults
		{
			get { return new RemoveTransformResultsCommand(this); }
		}

		public ICommand AddField
		{
			get { return new AddFieldCommand(this); }
		}

		public ICommand RemoveField
		{
			get { return new RemoveFieldCommand(this); }
		}

		public class AddMapCommand : Command
		{
			private readonly IndexDefinitionModel index;

			public AddMapCommand(IndexDefinitionModel index)
			{
				this.index = index;
			}

			public override void Execute(object parameter)
			{
				index.Maps.Add(new MapItem());
			}
		}

		public class RemoveMapCommand : Command
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

		public class AddReduceCommand : Command
		{
			private readonly IndexDefinitionModel index;

			public AddReduceCommand(IndexDefinitionModel index)
			{
				this.index = index;
			}

			public override void Execute(object parameter)
			{
				index.Reduce = string.Empty;
			}
		}

		public class RemoveReduceCommand : Command
		{
			private readonly IndexDefinitionModel index;

			public RemoveReduceCommand(IndexDefinitionModel index)
			{
				this.index = index;
			}

			public override void Execute(object parameter)
			{
				index.Reduce = null;
			}
		}

		public class AddTransformResultsCommand : Command
		{
			private readonly IndexDefinitionModel index;

			public AddTransformResultsCommand(IndexDefinitionModel index)
			{
				this.index = index;
			}

			public override void Execute(object parameter)
			{
				index.TransformResults = string.Empty;
			}
		}

		public class RemoveTransformResultsCommand : Command
		{
			private readonly IndexDefinitionModel index;

			public RemoveTransformResultsCommand(IndexDefinitionModel index)
			{
				this.index = index;
			}

			public override void Execute(object parameter)
			{
				index.TransformResults = null;
			}
		}

		public class AddFieldCommand : Command
		{
			private readonly IndexDefinitionModel index;

			public AddFieldCommand(IndexDefinitionModel index)
			{
				this.index = index;
			}

			public override void Execute(object parameter)
			{
				index.Fields.Add(new FieldProperties());
			}
		}

		public class RemoveFieldCommand : Command
		{
			private readonly IndexDefinitionModel index;

			public RemoveFieldCommand(IndexDefinitionModel index)
			{
				this.index = index;
			}

			public override void Execute(object parameter)
			{
				var field = parameter as FieldProperties;
				if (field == null || index.Fields.Contains(field) == false)
					return;

				index.Fields.Remove(field);
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
		}
	}
}