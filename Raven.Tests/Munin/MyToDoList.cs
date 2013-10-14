namespace Raven.Munin.Tests
{
	using System;
	using System.Linq;

	using Raven.Abstractions.Util.Encryptors;
	using Raven.Tests.Helpers;

	using Xunit;

	public class MyToDoList : IDisposable
	{
		public MyToDoList()
		{
			Encryptor.Initialize(SettingsHelper.UseFipsEncryptionAlgorithms);
		}

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

		public void Dispose()
		{
			Encryptor.Dispose();
		}
	}
}