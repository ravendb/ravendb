namespace Raven.Studio.Indexes
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using Caliburn.Micro;
	using Plugin;
	using Raven.Database.Indexing;

	public class EditIndexViewModel : Screen, IRavenScreen
	{
		readonly IndexDefinition index;
		readonly IDatabase database;
		bool isBusy;
		string name;

		public EditIndexViewModel(IndexDefinition index, IDatabase database)
		{
			DisplayName = "Edit Index";

			this.index = index;
			this.database = database;

			CompositionInitializer.SatisfyImports(this);

			Name = index.Name;
			LoadFields();
		}

		public SectionType Section
		{
			get { return SectionType.Indexes; }
		}

		public bool IsBusy
		{
			get { return isBusy; }
			set
			{
				isBusy = value;
				NotifyOfPropertyChange(() => IsBusy);
			}
		}

		public string Name
		{
			get { return name; }
			set
			{
				name = value;
				NotifyOfPropertyChange(() => Name);
			}
		}

		public IObservableCollection<FieldProperties> Fields { get; private set; }

		public string Map
		{
			get { return index.Map; }
			set { index.Map = value; }
		}

		public string Reduce
		{
			get { return index.Reduce; }
			set { index.Reduce = value; }
		}

		public string TransformResults
		{
			get { return index.TransformResults; }
			set { index.TransformResults = value; }
		}

		public void Save()
		{
			IsBusy = true;
			SaveFields();

			database.Session.Advanced.AsyncDatabaseCommands
				.PutIndexAsync(Name,index,true)
				.ContinueWith(task =>
			              		{
									IsBusy = false;
			              		});
		}

		public void Remove()
		{
			IsBusy = true;
			database.Session.Advanced.AsyncDatabaseCommands
				.DeleteIndexAsync(Name)
				.ContinueWith(task =>
				{
					IsBusy = false;
				});
		}

		void LoadFields()
		{
			Fields = new BindableCollection<FieldProperties>();

			CreateOrEditField(index.Indexes, (f, i) => f.Indexing = i);
			CreateOrEditField(index.Stores, (f, i) => f.Storage = i);
			CreateOrEditField(index.SortOptions, (f, i) => f.Sort = i);
			CreateOrEditField(index.Analyzers, (f, i) => f.Analyzer = i);
		}

		void CreateOrEditField<T>(IDictionary<string, T> dictionary, System.Action<FieldProperties, T> setter)
		{
			if (dictionary == null) return;

			foreach (var item in dictionary)
			{
				var localItem = item;
				var field = Fields.FirstOrDefault(f => f.Name == localItem.Key);
				if (field == null)
				{
					field = new FieldProperties {Name = localItem.Key};
					Fields.Add(field);
				}
				setter(field, localItem.Value);
			}
		}

		void SaveFields()
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
		}

		void AddField()
		{
			if (!Fields.Any(field => string.IsNullOrEmpty(field.Name)))
			{
				Fields.Add(new FieldProperties());
			}
		}
	}
}