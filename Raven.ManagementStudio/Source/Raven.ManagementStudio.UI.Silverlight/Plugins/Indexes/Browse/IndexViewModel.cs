using System.Collections.Generic;
using System.ComponentModel.Composition;
using Caliburn.Micro;
using Raven.Database.Indexing;
using Raven.ManagementStudio.Plugin;
using Raven.ManagementStudio.UI.Silverlight.Dialogs;
using Raven.ManagementStudio.UI.Silverlight.Messages;
using Raven.ManagementStudio.UI.Silverlight.Models;

namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Indexes.Browse
{
    public class IndexViewModel : Screen, IRavenScreen
    {
        private readonly Index _index;
        public IDatabase Database { get; private set; }

        public IndexViewModel(Index index, IDatabase database, IRavenScreen parent)
        {
            _index = index;
            Database = database;

            ParentRavenScreen = parent;
            DisplayName = "Edit Index";
            CompositionInitializer.SatisfyImports(this);
        }

        [Import]
        public IEventAggregator EventAggregator { get; set; }

        [Import]
        public IWindowManager WindowManager { get; set; }

        public IRavenScreen ParentRavenScreen { get; set; }

        public SectionType Section { get { return SectionType.Indexes; } }

        public void AddField()
        {
            _index.AddField();
        }

        public void RemoveField(FieldProperties field)
        {
            if (field != null)
            {
                _index.Fields.Remove(field);
            }
        }

        private bool _isBusy;
        public bool IsBusy
        {
            get { return _isBusy; }
            set
            {
                _isBusy = value;
                NotifyOfPropertyChange(() => IsBusy);
            }
        }

        public void ShowIndex()
        {
            EventAggregator.Publish(new ReplaceActiveScreen(this));
        }

        public string Name
        {
            get { return _index.Name; }
            set { _index.Name = value; }
        }

        public void Save()
        {
            IsBusy = true;
            _index.SaveFields();
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
            get { return _index.Fields; }
        }

        public string CurrentName
        {
            get { return _index.CurrentName; }
            set { _index.CurrentName = value; }
        }

        public IndexDefinition Definition
        {
            get { return _index.Definition; }
        }

        public string Map
        {
            get { return _index.Map; }
            set { _index.Map = value; }
        }

        public string Reduce
        {
            get { return _index.Reduce; }
            set { _index.Reduce = value; }
        }

        public string TransformResults
        {
            get { return _index.TransformResults; }
            set { _index.TransformResults = value; }
        }
    }
}
