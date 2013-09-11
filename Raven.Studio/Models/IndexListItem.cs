using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Windows;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Studio.Annotations;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class Group : INotifyPropertyChanged
	{
		public string GroupName { get; set; }
		public ObservableCollection<GroupItem> Items { get; set; }
		public Observable<bool> Collapse { get; set; }

		public ICommand ChangeCollapse
		{
			get { return new ActionCommand(() => Collapse.Value = !Collapse.Value); }
		}

		public string ItemCount
		{
			get { return string.Format(" ({0})",Items.Count); }
		}

		public Group(string groupName)
		{
			GroupName = groupName;
			Items = new ObservableCollection<GroupItem>();
			Collapse = new Observable<bool>();

			Items.CollectionChanged += (sender, args) => OnPropertyChanged("ItemCount");
		}

		public event PropertyChangedEventHandler PropertyChanged;

		[NotifyPropertyChangedInvocator]
		protected virtual void OnPropertyChanged(string propertyName)
		{
			var handler = PropertyChanged;
			if (handler != null) handler(this, new PropertyChangedEventArgs(propertyName));
		}
	}

	public abstract class GroupItem
	{
		public string Name { get; set; }
		public string GroupName { get; set; }
	}

	public class TransformerItem : GroupItem
	{
		public TransformerDefinition Transformer { get; set; }
	}

	public class IndexItem : GroupItem
	{
		public IndexStats IndexStats { get; set; }
		public string ModifiedName
		{
			get
			{
				var name = Name + " (" + IndexStats.DocsCount + ")";
				if (IndexStats.Priority.HasFlag(IndexingPriority.Abandoned))
					return name + " (abandoned)";

				if (IndexStats.Priority.HasFlag(IndexingPriority.Idle))
					return name + " (idle)";

				if (IndexStats.Priority.HasFlag(IndexingPriority.Disabled))
					return name + " (disabled)";

				return name;
			}
		}

		public bool CanPersist { get
		{
			return Name.StartsWith("Auto/") && IndexStats.IsOnRam != "false";
		} }

		public object LockImage
		{
			get
			{
				switch (IndexStats.LockMode)
				{
					case IndexLockMode.LockedIgnore:
						return Application.Current.Resources["Image_Lock_Tiny"];
					case IndexLockMode.LockedError:
						return Application.Current.Resources["Image_Lock_Error_Tiny"];
					case IndexLockMode.Unlock:
						return Application.Current.Resources["Image_Lock_Open_Tiny"];
					default:
						throw new ArgumentException("Unknown lock mode: " + IndexStats.LockMode);
				}
			}
		}

		public ICommand UnlockIndex { get { return new ChangeLockOfIndexCommand(Name, IndexLockMode.Unlock); } }
		public ICommand LockIgnoreIndex { get { return new ChangeLockOfIndexCommand(Name, IndexLockMode.LockedIgnore); } }
		public ICommand LockErrorIndex { get { return new ChangeLockOfIndexCommand(Name, IndexLockMode.LockedError); } }
		public ICommand MakeIndexPersisted
		{
			get
			{
				return new ActionCommand(() =>
				{
					var req = ApplicationModel
						.DatabaseCommands
						.CreateRequest("/indexes/" + Name + "?op=forceWriteToDisk" , "POST");
					req.ExecuteRequestAsync();
				});
			}
		}
		public bool CanEdit
		{
			get { return IndexStats.LockMode == IndexLockMode.Unlock; }
		}
	}
}