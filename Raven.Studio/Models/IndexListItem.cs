using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class IndexListItem
	{
	    public string Name { get; set; }
	}

	public class IndexGroupHeader : IndexListItem
	{
	}

	public class IndexItem : IndexListItem
	{
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

		public bool CanPersist { get { return Name.StartsWith("Auto/") && IndexStats.IsOnRam; } }

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