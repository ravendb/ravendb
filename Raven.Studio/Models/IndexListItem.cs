using System;
using System.Collections.Generic;
using System.Windows;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Studio.Commands;

namespace Raven.Studio.Models
{
	public class IndexGroup
	{
		public string GroupName { get; set; }
		public List<IndexItem> Indexes { get; set; }

		public IndexGroup(string groupName)
		{
			GroupName = groupName;
			Indexes = new List<IndexItem>();
		}
	}
	public class IndexItem
	{
		public string Name { get; set; }
		public string GroupName { get; set; }
		public IndexStats IndexStats { get; set; }
		public string ModifiedName
		{
			get
			{
				if (IndexStats.Priority.HasFlag(IndexingPriority.Abandoned))
					return Name + " (abandoned)";

				if (IndexStats.Priority.HasFlag(IndexingPriority.Idle))
					return Name + " (idle)";

				if (IndexStats.Priority.HasFlag(IndexingPriority.Disabled))
					return Name + " (disabled)";

				return Name;
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