using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Tasks;
using Raven.Database.Extensions;

namespace Raven.Database.Storage.StorageActions
{
	public partial class DocumentStorageActions 
	{
		public bool DoesTasksExistsForIndex(string name, DateTime? cutOff)
		{
			Api.JetSetCurrentIndex(session, Tasks, "by_index");
			Api.MakeKey(session, Tasks, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Tasks, SeekGrbit.SeekEQ) == false)
			{
				return false;
			}
            if (cutOff == null)
                return true;
            // we are at the first row for this index
		    var addedAt = Api.RetrieveColumnAsDateTime(session, Tasks, tableColumnsCache.TasksColumns["added_at"]).Value;
			return cutOff.Value > addedAt;
		}

	    public void AddTask(Task task)
		{
			int actualBookmarkSize;
			var bookmark = new byte[SystemParameters.BookmarkMost];
			using (var update = new Update(session, Tasks, JET_prep.Insert))
			{
				Api.SetColumn(session, Tasks, tableColumnsCache.TasksColumns["task"], task.AsString(), Encoding.Unicode);
				Api.SetColumn(session, Tasks, tableColumnsCache.TasksColumns["for_index"], task.Index, Encoding.Unicode);
				Api.SetColumn(session, Tasks, tableColumnsCache.TasksColumns["task_type"], task.Type, Encoding.Unicode);
				Api.SetColumn(session, Tasks, tableColumnsCache.TasksColumns["supports_merging"], task.SupportsMerging);
                Api.SetColumn(session, Tasks, tableColumnsCache.TasksColumns["added_at"], DateTime.Now);

				update.Save(bookmark, bookmark.Length, out actualBookmarkSize);
			}
			Api.JetGotoBookmark(session, Tasks, bookmark, actualBookmarkSize);
			if (logger.IsDebugEnabled)
				logger.DebugFormat("New task '{0}'", task.AsString());
		}


		public bool HasTasks
		{
			get
			{
				return Api.TryMoveFirst(session, Tasks);
			}
		}

		public int ApproximateTaskCount
		{
			get
			{
				if (Api.TryMoveFirst(session, Tasks) == false)
					return 0;
				var first = (int)Api.RetrieveColumnAsInt32(session, Tasks, tableColumnsCache.TasksColumns["id"]);
				if (Api.TryMoveLast(session, Tasks) == false)
					return 0;
				var last = (int)Api.RetrieveColumnAsInt32(session, Tasks, tableColumnsCache.TasksColumns["id"]);
				return last - first;
			}
		}

		public Task GetMergedTask(out int countOfMergedTasks)
		{
			Api.MoveBeforeFirst(session, Tasks);
			while (Api.TryMoveNext(session, Tasks))
			{
				var taskAsString = Api.RetrieveColumnAsString(session, Tasks, tableColumnsCache.TasksColumns["task"], Encoding.Unicode);
				try
				{
					Api.JetDelete(session, Tasks);
				}
				catch (EsentErrorException e)
				{
					if (e.Error != JET_err.WriteConflict)
						throw;
				}
				Task task;
				try
				{
					task = Task.ToTask(taskAsString);
				}
				catch (Exception e)
				{
					logger.ErrorFormat(e, "Could not create instance of a task: {0}", taskAsString);
					continue;
				}

				MergeSimilarTasks(task, out countOfMergedTasks);
				return task;
			}
			countOfMergedTasks = 0;
			return null;
		}

		private void MergeSimilarTasks(Task task, out int taskCount)
		{
			taskCount = 1;
			if (task.SupportsMerging == false)
				return;

			Api.JetSetCurrentIndex(session, Tasks, "mergables_by_task_type");
			Api.MakeKey(session, Tasks, true, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Tasks, task.Index, Encoding.Unicode, MakeKeyGrbit.None);
			Api.MakeKey(session, Tasks, task.Type, Encoding.Unicode, MakeKeyGrbit.None);
			// there are no tasks matching the current one, just return
			if (Api.TrySeek(session, Tasks, SeekGrbit.SeekEQ) == false)
			{
				return;
			}

			Api.MakeKey(session, Tasks, true, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Tasks, task.Index, Encoding.Unicode, MakeKeyGrbit.None);
			Api.MakeKey(session, Tasks, task.Type, Encoding.Unicode, MakeKeyGrbit.None);
			Api.JetSetIndexRange(session, Tasks, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
			do
			{
				// esent index ranges are approximate, and we need to check them ourselves as well
				if (Api.RetrieveColumnAsBoolean(session, Tasks, tableColumnsCache.TasksColumns["supports_merging"]) == false)
					continue;
				if (Api.RetrieveColumnAsString(session, Tasks, tableColumnsCache.TasksColumns["for_index"]) != task.Index)
					continue;
				if (Api.RetrieveColumnAsString(session, Tasks, tableColumnsCache.TasksColumns["task_type"]) != task.Type)
					continue;

				try
				{
					var taskAsString = Api.RetrieveColumnAsString(session, Tasks, tableColumnsCache.TasksColumns["task"], Encoding.Unicode);
					Task existingTask;
					try
					{
						existingTask = Task.ToTask(taskAsString);
					}
					catch (Exception e)
					{
						logger.ErrorFormat(e, "Could not create instance of a task: {0}", taskAsString);
						Api.JetDelete(session, Tasks);
						continue;
					}
					if (task.TryMerge(existingTask) == false)
						continue;
					Api.JetDelete(session, Tasks);
					taskCount += 1;
				}
				catch (EsentErrorException e)
				{
					if (e.Error == JET_err.WriteConflict)
						continue;
					throw;
				}
			} while (
					task.SupportsMerging &&
					Api.TryMoveNext(session, Tasks)
				);
		}

	}
}