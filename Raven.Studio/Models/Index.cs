namespace Raven.Studio.Models
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using Caliburn.Micro;
	using Raven.Database.Indexing;

	public class Index : PropertyChangedBase
	{
		string _name;

		public Index(string name, IndexDefinition definition)
		{
			Name = name;
			CurrentName = name;
			Definition = definition;

			LoadFields();
		}

		public string Name
		{
			get { return _name; }
			set
			{
				_name = value;
				NotifyOfPropertyChange(() => Name);
			}
		}

		public IObservableCollection<FieldProperties> Fields { get; private set; }

		public string CurrentName { get; set; }

		public string Map
		{
			get { return Definition.Map; }
			set { Definition.Map = value; }
		}

		public string Reduce
		{
			get { return Definition.Reduce; }
			set { Definition.Reduce = value; }
		}

		public string TransformResults
		{
			get { return Definition.TransformResults; }
			set { Definition.TransformResults = value; }
		}

		public IndexDefinition Definition { get; set; }

		void LoadFields()
		{
			Fields = new BindableCollection<FieldProperties>();

			CreateOrEditField(Definition.Indexes, (f, i) => f.Indexing = i);
			CreateOrEditField(Definition.Stores, (f, i) => f.Storage = i);
			CreateOrEditField(Definition.SortOptions, (f, i) => f.Sort = i);
			CreateOrEditField(Definition.Analyzers, (f, i) => f.Analyzer = i);
		}

		void CreateOrEditField<T>(IDictionary<string, T> dictionary, Action<FieldProperties, T> setter)
		{
			if (dictionary != null)
			{
				foreach (var item in dictionary)
				{
					KeyValuePair<string, T> localItem = item;
					FieldProperties field = Fields.FirstOrDefault(f => f.Name == localItem.Key);
					if (field == null)
					{
						field = new FieldProperties {Name = localItem.Key};
						Fields.Add(field);
					}
					setter(field, localItem.Value);
				}
			}
		}

		public void SaveFields()
		{
			Definition.Indexes.Clear();
			Definition.Stores.Clear();
			Definition.SortOptions.Clear();
			Definition.Analyzers.Clear();

			foreach (FieldProperties item in Fields)
			{
				if (item.Name != null)
				{
					Definition.Indexes[item.Name] = item.Indexing;
					Definition.Stores[item.Name] = item.Storage;
					Definition.SortOptions[item.Name] = item.Sort;
					Definition.Analyzers[item.Name] = item.Analyzer;
				}
			}
		}

		public void AddField()
		{
			if (!Fields.Any(field => string.IsNullOrEmpty(field.Name)))
			{
				Fields.Add(new FieldProperties());
			}
		}
	}
}