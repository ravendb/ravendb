using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Database.Indexing;

namespace Raven.Database.Tasks
{
	public abstract class Task
	{
		public string Index { get; set; }

		public string Type
		{
			get { return GetType().FullName; }
		}

		public virtual bool SupportsMerging
		{
			get
			{
				return true;
			}
		}

		public abstract bool TryMerge(Task task);

		public byte[] AsBytes()
		{
			var memoryStream = new MemoryStream();
			new JsonSerializer().Serialize(new BsonWriter(memoryStream), this);
			return memoryStream.ToArray();
		}

		public static Task ToTask(string taskType, byte[] task)
		{
			var type = typeof(Task).Assembly.GetType(taskType);
			return (Task) new JsonSerializer().Deserialize(new BsonReader(new MemoryStream(task)), type);
		}

		/// <summary>
		/// 	Tasks may NOT perform any writes operations on the TransactionalStorage!
		/// 	That is required because a failed task still commit  the TransactionalStorage 
		/// 	(to remove from the tasks).
		/// 	Another requirement is that executing task MUST be idempotent.
		/// </summary>
		public abstract void Execute(WorkContext context);

		public abstract Task Clone();
	}
}