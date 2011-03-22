namespace Raven.Studio.Features.Indexes
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using Caliburn.Micro;
	using Database;
	using Framework;
	using Messages;
	using Raven.Database.Indexing;

	public class EditIndexViewModel : RavenScreen
	{
		readonly IndexDefinition index;
		readonly IServer server;
		string name;
		bool shouldShowReduce;
		bool shouldShowTransformResults;

		public EditIndexViewModel(IndexDefinition index, IServer server, IEventAggregator events)
			: base(events)
		{
			DisplayName = "Edit Index";

			this.index = index;
			this.server = server;

			Name = index.Name;
			ShouldShowReduce = !string.IsNullOrEmpty(index.Reduce);
			ShouldShowTransformResults = !string.IsNullOrEmpty(index.TransformResults);
			AvailabeFields = new BindableCollection<string>(index.Fields);

			LoadFields();
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
		public IObservableCollection<string> AvailabeFields { get; private set; }

		public string Map
		{
			get { return index.Map; }
			set
			{
				index.Map = value;
				NotifyOfPropertyChange(() => Map);
			}
		}

		public string Reduce
		{
			get { return index.Reduce; }
			set
			{
				index.Reduce = value;
				NotifyOfPropertyChange(() => Reduce);
			}
		}

		public string TransformResults
		{
			get { return index.TransformResults; }
			set
			{
				index.TransformResults = value;
				NotifyOfPropertyChange(() => TransformResults);
			}
		}

		public bool ShouldShowReduce
		{
			get { return shouldShowReduce; }
			set
			{
				shouldShowReduce = value;
				NotifyOfPropertyChange(() => ShouldShowReduce);
			}
		}

		public bool ShouldShowTransformResults
		{
			get { return shouldShowTransformResults; }
			set
			{
				shouldShowTransformResults = value;
				NotifyOfPropertyChange(() => ShouldShowTransformResults);
			}
		}

		public void AddTransformResults()
		{
			ShouldShowTransformResults = true;
		}

		public void AddReduce()
		{
			ShouldShowReduce = true;
		}

		public void Save()
		{
			WorkStarted("saving index " + Name);
			SaveFields();

			if(string.IsNullOrEmpty(index.Reduce)) index.Reduce = null;
			if (string.IsNullOrEmpty(index.TransformResults)) index.TransformResults = null;

			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.PutIndexAsync(Name, index, true)
					.ContinueOnSuccess(task =>
					                   	{
					                   		WorkCompleted("saving index " + Name);
					                   		Events.Publish(new IndexUpdated {Index = this});
					                   	});
			}
		}

		public void Remove()
		{
			WorkStarted("removing index " + Name);
			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.DeleteIndexAsync(Name)
					.ContinueOnSuccess(task =>
					                   	{
					                   		WorkCompleted("removing index " + Name);
					                   		Events.Publish(new IndexUpdated {Index = this, IsRemoved = true});
					                   	});
			}
		}

		void LoadFields()
		{
			Fields = new BindableCollection<FieldProperties>();

			CreateOrEditField(index.Indexes, (f, i) => f.Indexing = i);
			CreateOrEditField(index.Stores, (f, i) => f.Storage = i);
			CreateOrEditField(index.SortOptions, (f, i) => f.Sort = i);
			CreateOrEditField(index.Analyzers, (f, i) => f.Analyzer = i);
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

				if(!string.IsNullOrEmpty(item.Analyzer))
					index.Analyzers[item.Name] = item.Analyzer;
			}
		}

		public void AddField()
		{
			if (Fields.Any(field => string.IsNullOrEmpty(field.Name))) return;

			Fields.Add(new FieldProperties());
		}

		public void RemoveField(FieldProperties field)
		{
			if (field == null || !Fields.Contains(field)) return;
			Fields.Remove(field);
		}
	}
}