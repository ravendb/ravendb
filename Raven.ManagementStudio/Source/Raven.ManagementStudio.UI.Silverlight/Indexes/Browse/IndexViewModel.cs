namespace Raven.ManagementStudio.UI.Silverlight.Indexes.Browse
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Messages;
	using Models;
	using Plugin;
	using Raven.Database.Indexing;

	public class IndexViewModel : Screen, IRavenScreen
	{
		readonly Index index;
		bool isBusy;

		public IndexViewModel(Index index, IDatabase database, IRavenScreen parent)
		{
			this.index = index;
			Database = database;

			ParentRavenScreen = parent;
			DisplayName = "Edit Index";
			CompositionInitializer.SatisfyImports(this);
		}

		public IDatabase Database { get; private set; }

		[Import]
		public IEventAggregator EventAggregator { get; set; }

		[Import]
		public IWindowManager WindowManager { get; set; }

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
			get { return index.Name; }
			set { index.Name = value; }
		}

		//private void HandleRemoveResult(Response<string> result)
		//{
		//    if (result.IsSuccess)
		//    {
		//        EventAggregator.Publish(new IndexChangeMessage { Index = this, IsRemoved = true });
		//    }
		//    else
		//    {
		//        WindowManager.ShowDialog(
		//            new InformationDialogViewModel("Error", result.Exception.Message));
		//    }

		//    IsBusy = false;
		//}

		public IObservableCollection<FieldProperties> Fields
		{
			get { return index.Fields; }
		}

		public string CurrentName
		{
			get { return index.CurrentName; }
			set { index.CurrentName = value; }
		}

		public IndexDefinition Definition
		{
			get { return index.Definition; }
		}

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

		public IRavenScreen ParentRavenScreen { get; set; }

		public SectionType Section
		{
			get { return SectionType.Indexes; }
		}

		public void AddField()
		{
			index.AddField();
		}

		public void RemoveField(FieldProperties field)
		{
			if (field != null)
			{
				index.Fields.Remove(field);
			}
		}

		public void ShowIndex()
		{
			EventAggregator.Publish(new ReplaceActiveScreen(this));
		}

		public void Save()
		{
			IsBusy = true;
			index.SaveFields();
			// Database.IndexSession.Save(Name, Definition, HandleSaveResult);
		}

		//private void HandleSaveResult(Response<KeyValuePair<string, IndexDefinition>> result)
		//{
		//    if (result.IsSuccess)
		//    {
		//        if (CurrentName != result.Data.Key)
		//        {
		//            var newIndex = new IndexViewModel(new Index(result.Data.Key, result.Data.Value), Database, this);

		//            EventAggregator.Publish(new IndexChangeMessage { Index = newIndex });
		//            Name = CurrentName;
		//        }
		//    }
		//    else
		//    {
		//        WindowManager.ShowDialog(
		//            new InformationDialogViewModel("Error", result.Exception.Message));
		//    }

		//    IsBusy = false;
		//}

		public void Remove()
		{
			IsBusy = true;
			//Database.IndexSession.Delete(Name, HandleRemoveResult);
		}
	}
}