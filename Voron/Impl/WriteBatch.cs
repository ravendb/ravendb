namespace Voron.Impl
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Linq;

	public class WriteBatch : IDisposable
	{
		private readonly List<BatchOperation> _operations;

		public IReadOnlyCollection<BatchOperation> Operations
		{
			get
			{
				return _operations;
			}
		}

		public long Size
		{
			get
			{
				return _operations.Sum(x =>
				{
					switch (x.Type)
					{
						case BatchOperationType.Add:
							return x.Value.Length + x.Key.Size;
						case BatchOperationType.MultiAdd:
							return x.MultiValue.Size + x.Key.Size;
						default:
							return x.Key.Size;
					}
				});
			}
		}

		public WriteBatch()
		{
			_operations = new List<BatchOperation>();
		}

		public void Add(Slice key, Stream value, string treeName, ushort? version = null)
		{
			if (treeName != null && treeName.Length == 0) throw new ArgumentException("treeName must not be empty", "treeName");
			if (value == null) throw new ArgumentNullException("value");
			if (value.Length == 0)
				throw new ArgumentException("Cannot add empty value");
			if (value.Length > int.MaxValue)
				throw new ArgumentException("Cannot add a value that is over 2GB in size", "value");

			_operations.Add(new BatchOperation(key, value, version, treeName, BatchOperationType.Add));
		}


		public void MultiAdd(Slice key, Slice value, string treeName, ushort? version = null)
		{
			if (treeName != null && treeName.Length == 0) throw new ArgumentException("treeName must not be empty", "treeName");
			if (value == null) throw new ArgumentNullException("value");
		
			_operations.Add(new BatchOperation(key, null, version, treeName, BatchOperationType.MultiAdd)
			{
				MultiValue = value
			});
		}

		public void Delete(Slice key, string treeName, ushort? version = null)
		{
			AssertValidRemove(treeName);

			_operations.Add(new BatchOperation(key, null, version, treeName, BatchOperationType.Delete));
		}

		public void MultiDelete(Slice key, Slice value, string treeName, ushort? version = null)
		{
			AssertValidRemove(treeName);

			_operations.Add(new BatchOperation(key, null, version, treeName, BatchOperationType.MultiDelete)
			{
				MultiValue = value
			});
		}


		private static void AssertValidRemove(string treeName)
		{
			if (treeName != null && treeName.Length == 0) throw new ArgumentException("treeName must not be empty", "treeName");
		}

		public void MultiAdd(Slice key, Stream value, string treeName, ushort? version = null)
		{
			AssertValidMultiOperation(value, treeName);

			_operations.Add(new BatchOperation(key, value, version, treeName, BatchOperationType.MultiAdd));
		}

		private static void AssertValidMultiOperation(Stream value, string treeName)
		{
			if (treeName != null && treeName.Length == 0) throw new ArgumentException("treeName must not be empty", "treeName");
			if (value == null) throw new ArgumentNullException("value");
			if (value.Length == 0)
				throw new ArgumentException("Cannot add empty value");
			if (value.Length > int.MaxValue)
				throw new ArgumentException("Cannot add a value that is over 2GB in size", "value");
		}

		public void MultiDelete(Slice key, Stream value, string treeName, ushort? version = null)
		{
			AssertValidMultiOperation(value, treeName);

			_operations.Add(new BatchOperation(key, value, version, treeName, BatchOperationType.MultiAdd));
		}

		public class BatchOperation
		{
			private readonly long originalStreamPosition;

			public BatchOperation(Slice key, Stream value, ushort? version, string treeName, BatchOperationType type)
			{
				Key = key;
				Value = value;
				Version = version;
				TreeName = treeName;
				Type = type;

				if (value != null)
					originalStreamPosition = value.Position;
			}

			public Slice Key { get; private set; }

			public Stream Value { get; private set; }

			public string TreeName { get; private set; }

			public BatchOperationType Type { get; private set; }

			public ushort? Version { get; private set; }

			public Slice MultiValue { get; set; }

			public void Reset()
			{
				if (Value != null)
					Value.Position = originalStreamPosition;
			}
		}

		public enum BatchOperationType
		{
			Add,
			Delete,
			MultiAdd,
			MultiDelete
		}

		public void Dispose()
		{
			foreach (var operation in _operations)
			{
				using (operation.Value)
				{

				}
			}
		}
	}
}
