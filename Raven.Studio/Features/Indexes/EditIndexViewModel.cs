using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;

namespace Raven.Studio.Features.Indexes
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading.Tasks;
	using Caliburn.Micro;
	using Client.Extensions;
	using Documents;
	using Framework;
	using Framework.Extensions;
	using Messages;
	using Plugins;

    public class EditIndexViewModel : RavenScreen
	{
		readonly IndexDefinition index;
		readonly IServer server;
		bool isDirty;
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

			Fields = new BindableCollection<FieldProperties>();

			CreateOrEditField(index.Indexes, (f, i) => f.Indexing = i);
			CreateOrEditField(index.Stores, (f, i) => f.Storage = i);
			CreateOrEditField(index.SortOptions, (f, i) => f.Sort = i);
			CreateOrEditField(index.Analyzers, (f, i) => f.Analyzer = i);

			Fields.CollectionChanged += (s, e) => { IsDirty = true; };

			QueryResults = new BindablePagedQuery<DocumentViewModel>(
				(start, size) => { throw new Exception("Replace this when executing the query."); });

			RelatedErrors = (from error in this.server.Errors
							 where error.Index == index.Name
							 select error).ToList();
		}

		string status;
		public string Status
		{
			get { return status; }
			set
			{
				status = value;
				NotifyOfPropertyChange(() => Status);
			}
		}

		public IEnumerable<ServerError> RelatedErrors { get; private set; }

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
				CheckForDirt(index.Map, value);
				index.Map = value;
				NotifyOfPropertyChange(() => Map);
			}
		}

		public string Reduce
		{
			get { return index.Reduce; }
			set
			{
				if (index.Reduce == null && string.IsNullOrEmpty(value)) return;
				CheckForDirt(index.Reduce, value);
				index.Reduce = value;
				NotifyOfPropertyChange(() => Reduce);
			}
		}

		public string TransformResults
		{
			get { return index.TransformResults; }
			set
			{
				if (index.TransformResults == null && string.IsNullOrEmpty(value)) return;
				CheckForDirt(index.TransformResults, value);
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

		public bool IsDirty
		{
			get { return isDirty; }
			set
			{
				isDirty = value;
				NotifyOfPropertyChange(() => IsDirty);
			}
		}

		public BindablePagedQuery<DocumentViewModel> QueryResults { get; private set; }

		void CheckForDirt<T>(T oldValue, T newValue)
		{
			if (newValue.Equals(oldValue)) return;
			IsDirty = true;
		}

		public void AddTransformResults() { ShouldShowTransformResults = true; }

		public void AddReduce() { ShouldShowReduce = true; }

		public void Save()
		{
			Status = string.Empty;
			WorkStarted("saving index " + Name);
			SaveFields();

			if (string.IsNullOrEmpty(index.Reduce)) index.Reduce = null;
			if (string.IsNullOrEmpty(index.TransformResults)) index.TransformResults = null;

			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.PutIndexAsync(Name, index, true)
					.ContinueWith(
					task =>
					{
						IsDirty = false;
						WorkCompleted("saving index " + Name);
						Events.Publish(new IndexUpdated { Index = this });
					},
					faulted =>
					{
						WorkCompleted("saving index " + Name);
						var error = faulted.Exception.ExtractSingleInnerException().SimplifyError();
						Status = error;
						NotifyError("An error occured while attempting to save " + Name);
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
											Events.Publish(new IndexUpdated { Index = this, IsRemoved = true });
										});
			}
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

		void SaveFields()
		{
			QueryResults.ClearResults();
			QueryResultsStatus = string.Empty;

			index.Indexes.Clear();
			index.Stores.Clear();
			index.SortOptions.Clear();
			index.Analyzers.Clear();

			foreach (var item in Fields.Where(item => item.Name != null))
			{
				index.Indexes[item.Name] = item.Indexing;
				index.Stores[item.Name] = item.Storage;
				index.SortOptions[item.Name] = item.Sort;

				if (!string.IsNullOrEmpty(item.Analyzer))
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

		public void QueryAgainstIndex()
		{
			QueryResults.Query = BuildQuery;
			QueryResults.LoadPage();
		}

		string queryResultsStatus;
		public string QueryResultsStatus
		{
			get { return queryResultsStatus; }
			set
			{
				queryResultsStatus = value;
				NotifyOfPropertyChange(() => QueryResultsStatus);
			}
		}

		static string DetermineResultsStatus(QueryResult result)
		{
			//TODO: give the user some info about skipped results, etc?
			if (result.TotalResults == 0) return "No documents matched the query.";
			if (result.TotalResults == 1) return "1 document found.";
			return string.Format("{0} documents found.", result.TotalResults);
		}

		Task<DocumentViewModel[]> BuildQuery(int start, int pageSize)
		{
			using (var session = server.OpenSession())
			{
				var indexName = Name;
				var query = new IndexQuery
								{
									Start = start,
									PageSize = pageSize,
								};

				return session.Advanced.AsyncDatabaseCommands
					.QueryAsync(indexName, query, null)
					.ContinueWith(x =>
									{
										QueryResults.GetTotalResults = () => x.Result.TotalResults;

										QueryResultsStatus = DetermineResultsStatus(x.Result);

										return x.Result.Results
											.Select(obj => new DocumentViewModel(obj.ToJsonDocument()))
											.ToArray();
									});
			}
		}
	}
}