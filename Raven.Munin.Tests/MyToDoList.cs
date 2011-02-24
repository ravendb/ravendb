using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Xunit;

namespace Raven.Munin.Tests
{
	public class MyToDoList
	{
		[Fact]
		public void CanStoreToDoItems()
		{
			var repository = new ToDoRepository(new MemoryPersistentSource());

			repository.Save(new ToDo
			{
				Date = DateTime.Today.AddDays(2),
				Action = "Buy Milk"
			});
		}

		[Fact]
		public void CanReadToDoItems()
		{
			var repository = new ToDoRepository(new MemoryPersistentSource());

			var guid = repository.Save(new ToDo
			{
				Date = DateTime.Today.AddDays(2),
				Action = "Buy Milk"
			});

			var todo = repository.Get(guid);
			Assert.Equal("Buy Milk", todo.Action);
		}

		[Fact]
		public void CanIterate()
		{
			var repository = new ToDoRepository(new MemoryPersistentSource());

			for (int i = 0; i < 10; i++)
			{
				repository.Save(new ToDo
				{
					Date = DateTime.Today.AddDays(2),
					Action = "Buy Milk #" + i
				});

			}
			Assert.Equal(10, repository.All().Count());
		}

		[Fact]
		public void CanQuery()
		{
			var repository = new ToDoRepository(new MemoryPersistentSource());

			for (int i = 0; i < 10; i++)
			{
				repository.Save(new ToDo
				{
					Date = DateTime.Today.AddDays(2),
					Action = "Buy Milk #" + i
				});

			}
			var results = repository.All().OrderByDescending(toDo=>toDo.Action).Take(3).ToArray();
			Assert.Equal("Buy Milk #9", results[0].Action);
			Assert.Equal("Buy Milk #8", results[1].Action);
			Assert.Equal("Buy Milk #7", results[2].Action);

		}
	}

	public class ToDo
	{
		public string Action { get; set; }
		public DateTime Date { get; set; }

		public override string ToString()
		{
			return Action;
		}
	}

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