namespace Voron.Impl
{
	using System;
	using System.Collections.Generic;
	using System.Collections.ObjectModel;
	using System.IO;
	using System.Linq;

	public class WriteBatch : IDisposable
	{
		private readonly List<BatchOperation> _operations;

		public ReadOnlyCollection<BatchOperation> Operations
		{
			get
			{
				return _operations.AsReadOnly();
			}
		}

		public long Size
		{
			get
			{
				return _operations.Sum(x => x.Type == BatchOperationType.Add ? x.ValueSize + x.Key.Size : x.Key.Size);
			}
		}

	    public Dictionary<Slice, ReadResult> GetAddedValues(string treeName)
	    {
	        return GetItemsByOperationTypeAndTreeName(BatchOperationType.Add,treeName);
	    }

        public Dictionary<Slice, ReadResult> GetAddedMultiValues(string treeName)
	    {
	        return GetItemsByOperationTypeAndTreeName(BatchOperationType.MultiAdd, treeName);
	    }

        public Dictionary<Slice, ReadResult> GetDeletedValues(string treeName)
	    {
	        return GetItemsByOperationTypeAndTreeName(BatchOperationType.Delete, treeName);
	    }

        public Dictionary<Slice, ReadResult> GetDeletedMultiValues(string treeName)
	    {
	        return GetItemsByOperationTypeAndTreeName(BatchOperationType.MultiDelete, treeName);
	    }

	    public WriteBatch()
		{
			_operations = new List<BatchOperation>();
		}

	    public bool ContainsChangedValue(Slice key, BatchOperationType operationType)
	    {
	        return _operations.Any(operation => operation.Key.Equals(key) && operation.Type == operationType);
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

		public void Delete(Slice key, string treeName, ushort? version = null)
		{
			AssertValidRemove(treeName);

			_operations.Add(new BatchOperation(key, null as Stream, version, treeName, BatchOperationType.Delete));
		}

		private static void AssertValidRemove(string treeName)
		{
			if (treeName != null && treeName.Length == 0) throw new ArgumentException("treeName must not be empty", "treeName");
		}

		public void MultiAdd(Slice key, Slice value, string treeName, ushort? version = null)
		{
			AssertValidMultiOperation(value, treeName);

			_operations.Add(new BatchOperation(key, value, version, treeName, BatchOperationType.MultiAdd));
		}

		private static void AssertValidMultiOperation(Slice value, string treeName)
		{
			if (treeName != null && treeName.Length == 0) throw new ArgumentException("treeName must not be empty", "treeName");
			if (value == null) throw new ArgumentNullException("value");
			if (value.Size == 0)
				throw new ArgumentException("Cannot add empty value");
			if (value.Size > int.MaxValue)
				throw new ArgumentException("Cannot add a value that is over 2GB in size", "value");
		}

		public void MultiDelete(Slice key, Slice value, string treeName, ushort? version = null)
		{
			AssertValidMultiOperation(value, treeName);

			_operations.Add(new BatchOperation(key, value, version, treeName, BatchOperationType.MultiDelete));
		}

		public class BatchOperation
		{
			private readonly long originalStreamPosition;

			private readonly Action reset = delegate { };

			public BatchOperation(Slice key, Stream value, ushort? version, string treeName, BatchOperationType type)
				: this(key, value as object, version, treeName, type)
			{
				if (value != null)
				{
					originalStreamPosition = value.Position;
					ValueSize = value.Length;

					reset = () => value.Position = originalStreamPosition;
				}
			}

			public BatchOperation(Slice key, Slice value, ushort? version, string treeName, BatchOperationType type)
				: this(key, value as object, version, treeName, type)
			{
				if (value != null)
				{
					originalStreamPosition = 0;
					ValueSize = value.Size;
				}
			}

			private BatchOperation(Slice key, object value, ushort? version, string treeName, BatchOperationType type)
			{
				Key = key;
				Value = value;
				Version = version;
				TreeName = treeName;
				Type = type;
			}

			public Slice Key { get; private set; }

			public long ValueSize { get; private set; }

			public object Value { get; private set; }

			public string TreeName { get; private set; }

			public BatchOperationType Type { get; private set; }

			public ushort? Version { get; private set; }

			public void Reset()
			{
				reset();
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
				var disposable = operation.Value as IDisposable;
				if (disposable != null)
					disposable.Dispose();
			}
		}

        private Dictionary<Slice, ReadResult> GetItemsByOperationTypeAndTreeName(BatchOperationType operationType, string treeName)
        {
            return _operations.Where(operation => operation.Type == operationType && operation.TreeName == treeName)
                              .ToDictionary(operation => operation.Key, operation => new ReadResult(operation.Value as Stream,operation.Version ?? 0));
        }

	}
}
