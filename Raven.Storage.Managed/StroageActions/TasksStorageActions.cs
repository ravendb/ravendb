using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Database.Storage.StorageActions;
using Raven.Database.Tasks;
using System.Linq;

namespace Raven.Storage.Managed.StroageActions
{
	public class TasksStorageActions : AbstractStorageActions, ITasksStorageActions
	{
		public bool DoesTasksExistsForIndex(string name, DateTime? cutOff)
		{
			foreach (var treeNode in Viewer.TasksByIndex
				.ScanFromInclusive(new JObject(new JProperty("Index", name)))
				.TakeWhile(x=>x.NodeKey.Value<string>("Index") == name))
			{
				if (cutOff == null)
					return true;
				if (treeNode.NodeValue == null)
					continue;
				var timestamp = DateTime.FromBinary(treeNode.NodeValue.Value);
				if (timestamp < cutOff.Value)
					return true;
			}
			return false;
		}

		public void AddTask(Task task)
		{
			var pos = Writer.Position;
			BinaryWriter.Write(task.Type);
			new JsonSerializer().Serialize(new BsonWriter(Writer), task);
			Mutator.IncrementTaskCount();
			task.Id = Mutator.Tasks.Enqueue(pos);
			Mutator.TasksByIndex.Add(new JObject(
			                         	new JProperty("Index", task.Index),
			                         	new JProperty("Id", task.Id)
			                         	),
			                         DateTime.UtcNow.ToBinary());
		}

		public bool HasTasks
		{
			get { return Viewer.TaskCount > 0; }
		}

		public long ApproximateTaskCount
		{
			get { return Viewer.TaskCount; }
		}

		public Task GetMergedTask(out int countOfMergedTasks)
		{
			countOfMergedTasks = 0;
			Task task = null;
			var idsToRemove = new List<long>();
			foreach (var tuple in Mutator.Tasks.Scan())
			{
				Reader.Position = tuple.Item1;
				var taskTypeName = BinaryReader.ReadString();
				var taskType = Type.GetType(taskTypeName);

				if (task != null && task.GetType() != taskType)
				{
					break;
				}
				
				var currentTask = (Task) new JsonSerializer().Deserialize(new BsonReader(Reader), taskType);

				if (task == null)
				{
					countOfMergedTasks += 1;
					task = currentTask;
					idsToRemove.Add(tuple.Item2);
					continue;
				}

				if (task.SupportsMerging == false)
					break;

				if (task.TryMerge(currentTask) == false)
					break;

				idsToRemove.Add(tuple.Item2);
				countOfMergedTasks += 1;
			}
			foreach (var idToRemove in idsToRemove)
			{
				Mutator.Tasks.Remove(idToRemove);
			}
			return task;
		}

	}
}