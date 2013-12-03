namespace Voron.Impl
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Linq;

	public class WriteBatch : IDisposable
	{
		private readonly Dictionary<string, Dictionary<Slice, BatchOperation>> _lastOperations;

		private readonly SliceEqualityComparer _sliceEqualityComparer;

		public List<BatchOperation> Operations
		{
			get
			{
				return _lastOperations
					.SelectMany(x => x.Value.Values.OrderBy(op => op.Key, _sliceEqualityComparer))
					.ToList();
			}
		}

		public Func<long> Size
		{
			get
			{
				return () => _lastOperations.Sum(operation => operation.Value.Values.Sum(x => x.Type == BatchOperationType.Add ? x.ValueSize + x.Key.Size : x.Key.Size));
			}
		}

		internal bool TryGetValue(string treeName, Slice key, out Stream value, out ushort? version, out BatchOperationType operationType)
		{
		    value = null;
		    version = null;
			operationType = BatchOperationType.None;

			if (treeName == null)
				treeName = Constants.RootTreeName;

			Dictionary<Slice, BatchOperation> operations;
			if (_lastOperations.TryGetValue(treeName, out operations) == false)
				return false;

			BatchOperation operation;
			if (operations.TryGetValue(key, out operation))
			{
				operationType = operation.Type;
			    version = operation.Version;

				if (operation.Type == BatchOperationType.Delete)
					return true;

				if (operation.Type == BatchOperationType.MultiDelete)
					return true;

			    value = operation.Value as Stream;

				if (operation.Type == BatchOperationType.Add)
					return true;

				if (operation.Type == BatchOperationType.MultiAdd)
					return true;
			}

			return false;
		}

		public WriteBatch()
		{
			_lastOperations = new Dictionary<string, Dictionary<Slice, BatchOperation>>();
			_sliceEqualityComparer = new SliceEqualityComparer();
		}

		public void Add(Slice key, Stream value, string treeName, ushort? version = null)
		{
			if (treeName != null && treeName.Length == 0) throw new ArgumentException("treeName must not be empty", "treeName");
			if (value == null) throw new ArgumentNullException("value");
			//TODO : check up if adding empty values make sense in Voron --> in order to be consistent with existing behavior of Esent, this should be allowed
			//			if (value.Length == 0)
			//				throw new ArgumentException("Cannot add empty value");
			if (value.Length > int.MaxValue)
				throw new ArgumentException("Cannot add a value that is over 2GB in size", "value");


			AddOperation(new BatchOperation(key, value, version, treeName, BatchOperationType.Add));
		}

		public void Delete(Slice key, string treeName, ushort? version = null)
		{
			AssertValidRemove(treeName);

			AddOperation(new BatchOperation(key, null as Stream, version, treeName, BatchOperationType.Delete));
		}

		private static void AssertValidRemove(string treeName)
		{
			if (treeName != null && treeName.Length == 0) throw new ArgumentException("treeName must not be empty", "treeName");
		}

		public void MultiAdd(Slice key, Slice value, string treeName, ushort? version = null)
		{
			AssertValidMultiOperation(value, treeName);

			AddOperation(new BatchOperation(key, value, version, treeName, BatchOperationType.MultiAdd));
		}

		private static void AssertValidMultiOperation(Slice value, string treeName)
		{
			if (treeName != null && treeName.Length == 0) throw new ArgumentException("treeName must not be empty", "treeName");
			if (value == null) throw new ArgumentNullException("value");
			if (value.Size == 0)
				throw new ArgumentException("Cannot add empty value");
		}

		public void MultiDelete(Slice key, Slice value, string treeName, ushort? version = null)
		{
			AssertValidMultiOperation(value, treeName);

			AddOperation(new BatchOperation(key, value, version, treeName, BatchOperationType.MultiDelete));
		}

		private void AddOperation(BatchOperation operation)
		{
			var treeName = operation.TreeName;
			if (treeName != null && treeName.Length == 0) throw new ArgumentException("treeName must not be empty", "treeName");

			if (treeName == null)
				treeName = Constants.RootTreeName;

			Dictionary<Slice, BatchOperation> lastOpsForTree;
			if (_lastOperations.TryGetValue(treeName, out lastOpsForTree) == false)
			{
				_lastOperations[treeName] = lastOpsForTree = new Dictionary<Slice, BatchOperation>(_sliceEqualityComparer);
			}
			BatchOperation old;
			if (lastOpsForTree.TryGetValue(operation.Key, out old))
			{
				operation.SetVersionFrom(old);
				var disposable = old.Value as IDisposable;
				if (disposable != null)
					disposable.Dispose();
			}
			lastOpsForTree[operation.Key] = operation;
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

			public void SetVersionFrom(BatchOperation other)
			{
				if (other.Version != null && 
					other.Version + 1 == Version)
					Version = other.Version;
			}

			public void Reset()
			{
				reset();
			}
		}

		public enum BatchOperationType
		{
			None = 0,
			Add = 1,
			Delete = 2,
			MultiAdd = 3,
			MultiDelete = 4,
		}

		public void Dispose()
		{
			foreach (var operation in _lastOperations)
			{
				foreach (var val in operation.Value)
				{
					var disposable = val.Value.Value as IDisposable;
					if (disposable != null)
						disposable.Dispose();
				}

			}

			_lastOperations.Clear();
		}
	}
}