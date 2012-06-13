using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Munin.Tests
{
	public class ToDoRepository
	{
		readonly Database database;
		private Table todos;

		public ToDoRepository(IPersistentSource source)
		{
			database = new Database(source);
			todos = database.Add(new Table("todo"));
			database.Initialze();
		}

		public Guid Save(ToDo todo)
		{
			database.BeginTransaction();

			var id = Guid.NewGuid();

			var ms = new MemoryStream();
			var jsonTextWriter = new JsonTextWriter(new StreamWriter(ms));
			new JsonSerializer().Serialize(
				jsonTextWriter,
				todo
				);
			jsonTextWriter.Flush();

			todos.Put(id.ToByteArray(), ms.ToArray());

			database.Commit();

			return id;
		}

		public ToDo Get(Guid guid)
		{
			var readResult = todos.Read(guid.ToByteArray());
			if (readResult == null)
				return null;
			var bytes = readResult.Data();

			return ConvertToToDo(bytes);
		}

		private static ToDo ConvertToToDo(byte[] bytes)
		{
			var memoryStream = new MemoryStream(bytes);
			return new JsonSerializer().Deserialize<ToDo>(new JsonTextReader(new StreamReader(memoryStream)));
		}

		public IEnumerable<ToDo> All()
		{
			return from item in todos
				   select ConvertToToDo(item.Data());
		}
	}
}