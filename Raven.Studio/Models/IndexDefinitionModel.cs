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
						
			OnEverythingChanged();
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

		public ObservableCollection<MapItem> Maps { get; private set; }

		public ICommand AddMap
		{
			get { return new AddMapCommand(this); }
		}

		public ICommand RemoveMap
		{
			get { return new RemoveMapCommand(this); }
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

		public class MapItem
		{
			public string Text { get; set; }
		}
	}
}