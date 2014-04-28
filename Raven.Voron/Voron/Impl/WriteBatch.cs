using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

using Voron.Exceptions;

namespace Voron.Impl
{
	public class WriteBatch : IDisposable
	{
		private readonly Dictionary<string, Dictionary<Slice, BatchOperation>> _lastOperations;
		private readonly Dictionary<string, Dictionary<Slice, List<BatchOperation>>> _multiTreeOperations;

		private readonly SliceEqualityComparer _sliceEqualityComparer;
		private bool _disposeAfterWrite = true;

		public IEnumerable<BatchOperation> Operations
		{
			get
			{
				var allOperations = _lastOperations.SelectMany(x => x.Value.Values);

				if (_multiTreeOperations.Count == 0)
					return allOperations;

				return allOperations.Concat(_multiTreeOperations.SelectMany(x => x.Value.Values)
												.SelectMany(x => x));
			}
		}

		public long Size()
		{
			long totalSize = 0;

			if (_lastOperations.Count > 0)
				totalSize += _lastOperations.Sum(
					operation =>
					operation.Value.Values.Sum(x => x.Type == BatchOperationType.Add ? x.ValueSize + x.Key.Size : x.Key.Size));

			if (_multiTreeOperations.Count > 0)
				totalSize += _multiTreeOperations.Sum(
					tree =>
					tree.Value.Sum(
						multiOp => multiOp.Value.Sum(x => x.Type == BatchOperationType.Add ? x.ValueSize + x.Key.Size : x.Key.Size)));
			return totalSize;
		}

		public bool IsEmpty { get { return _lastOperations.Count == 0 && _multiTreeOperations.Count == 0; } }

		public bool DisposeAfterWrite
		{
			get { return _disposeAfterWrite; }
			set { _disposeAfterWrite = value; }
		}

		internal bool TryGetValue(string treeName, Slice key, out Stream value, out ushort? version, out BatchOperationType operationType)
		{
			value = null;
			version = null;
			operationType = BatchOperationType.None;

			if (treeName == null)
				treeName = Constants.RootTreeName;

			//first check if it is a multi-tree operation
			Dictionary<Slice, List<BatchOperation>> treeOperations;
			if (_multiTreeOperations.TryGetValue(treeName, out treeOperations))
			{
				List<BatchOperation> operationRecords;
				if (treeOperations.TryGetValue(key, out operationRecords))
				{
					//since in multi-tree there are many operations for single tree key, then fetching operation type and value is meaningless
					return true;
				}
			}

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

				value = operation.Value as Stream;
				operation.Reset(); // will reset stream position

				if (operation.Type == BatchOperationType.Add)
					return true;

			}

			return false;
		}

		public WriteBatch()
		{
			_lastOperations = new Dictionary<string, Dictionary<Slice, BatchOperation>>();
			_multiTreeOperations = new Dictionary<string, Dictionary<Slice, List<BatchOperation>>>();
			_sliceEqualityComparer = new SliceEqualityComparer();
		}

		public void Add(Slice key, Slice value, string treeName, ushort? version = null, bool shouldIgnoreConcurrencyExceptions = false)
		{
			AssertValidTreeName(treeName);
			if (value == null) throw new ArgumentNullException("value");

			var batchOperation = BatchOperation.Add(key, value, version, treeName);
			if (shouldIgnoreConcurrencyExceptions)
				batchOperation.SetIgnoreExceptionOnExecution<ConcurrencyException>();
			AddOperation(batchOperation);
		}

		public void Add(Slice key, Stream value, string treeName, ushort? version = null, bool shouldIgnoreConcurrencyExceptions = false)
		{
			AssertValidTreeName(treeName);
			if (value == null) throw new ArgumentNullException("value");
			if (value.Length > int.MaxValue)
				throw new ArgumentException("Cannot add a value that is over 2GB in size", "value");

			var batchOperation = BatchOperation.Add(key, value, version, treeName);
			if (shouldIgnoreConcurrencyExceptions)
				batchOperation.SetIgnoreExceptionOnExecution<ConcurrencyException>();
			AddOperation(batchOperation);
		}

		public void Delete(Slice key, string treeName, ushort? version = null)
		{
			AssertValidRemove(treeName);

			AddOperation(BatchOperation.Delete(key, version, treeName));
		}

		private static void AssertValidRemove(string treeName)
		{
			AssertValidTreeName(treeName);
		}

		public void MultiAdd(Slice key, Slice value, string treeName, ushort? version = null)
		{
			AssertValidMultiOperation(value, treeName);

			AddOperation(BatchOperation.MultiAdd(key, value, version, treeName));
		}

		public void MultiDelete(Slice key, Slice value, string treeName, ushort? version = null)
		{
			AssertValidMultiOperation(value, treeName);

			AddOperation(BatchOperation.MultiDelete(key, value, version, treeName));
		}

		public void Increment(Slice key, long delta, string treeName, ushort? version = null)
		{
			AssertValidTreeName(treeName);

			AddOperation(BatchOperation.Increment(key, delta, version, treeName));
		}

		private static void AssertValidTreeName(string treeName)
		{
			if (treeName != null && treeName.Length == 0) 
				throw new ArgumentException("treeName must not be empty", "treeName");
		}

		private static void AssertValidMultiOperation(Slice value, string treeName)
		{
			AssertValidTreeName(treeName);
			if (value == null) throw new ArgumentNullException("value");
			if (value.Size == 0)
				throw new ArgumentException("Cannot add empty value");
		}

		private void AddOperation(BatchOperation operation)
		{
			var treeName = operation.TreeName;
			AssertValidTreeName(treeName);

			if (treeName == null)
				treeName = Constants.RootTreeName;

			if (operation.Type == BatchOperationType.MultiAdd || operation.Type == BatchOperationType.MultiDelete)
			{
				Dictionary<Slice, List<BatchOperation>> multiTreeOperationsOfTree;
				if (_multiTreeOperations.TryGetValue(treeName, out multiTreeOperationsOfTree) == false)
				{
					_multiTreeOperations[treeName] =
						multiTreeOperationsOfTree = new Dictionary<Slice, List<BatchOperation>>(_sliceEqualityComparer);
				}

				List<BatchOperation> specificMultiTreeOperations;
				if (multiTreeOperationsOfTree.TryGetValue(operation.Key, out specificMultiTreeOperations) == false)
					multiTreeOperationsOfTree[operation.Key] = specificMultiTreeOperations = new List<BatchOperation>();

				specificMultiTreeOperations.Add(operation);
			}
			else
			{
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
		}

		public class BatchOperation : IComparable<BatchOperation>
		{
#if DEBUG
			private readonly StackTrace stackTrace;
			public StackTrace StackTrace
			{
				get { return stackTrace; }
			}
#endif

			private readonly long originalStreamPosition;
			private readonly HashSet<Type> exceptionTypesToIgnore = new HashSet<Type>();
			private readonly Action reset = delegate { };
			private readonly Slice valSlice;

			public static BatchOperation Add(Slice key, Slice value, ushort? version, string treeName)
			{
				return new BatchOperation(key, value, version, treeName, BatchOperationType.Add);
			}

			public static BatchOperation Add(Slice key, Stream stream, ushort? version, string treeName)
			{
				return new BatchOperation(key, stream, version, treeName, BatchOperationType.Add);
			}

			public static BatchOperation Delete(Slice key, ushort? version, string treeName)
			{
				return new BatchOperation(key, null as Stream, version, treeName, BatchOperationType.Delete);
			}

			public static BatchOperation MultiAdd(Slice key, Slice value, ushort? version, string treeName)
			{
				return new BatchOperation(key, value, version, treeName, BatchOperationType.MultiAdd);
			}

			public static BatchOperation MultiDelete(Slice key, Slice value, ushort? version, string treeName)
			{
				return new BatchOperation(key, value, version, treeName, BatchOperationType.MultiDelete);
			}

			public static BatchOperation Increment(Slice key, long delta, ushort? version, string treeName)
			{
				return new BatchOperation(key, delta, version, treeName, BatchOperationType.Increment);
			}

			private BatchOperation(Slice key, Stream value, ushort? version, string treeName, BatchOperationType type)
				: this(key, value as object, version, treeName, type)
			{
				if (value != null)
				{
					originalStreamPosition = value.Position;
					ValueSize = value.Length;

					reset = () => value.Position = originalStreamPosition;
				}

#if DEBUG
				stackTrace = new StackTrace();
#endif
			}

			private BatchOperation(Slice key, Slice value, ushort? version, string treeName, BatchOperationType type)
				: this(key, value as object, version, treeName, type)
			{
				if (value != null)
				{
					valSlice = value;
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

			public HashSet<Type> ExceptionTypesToIgnore
			{
				get { return exceptionTypesToIgnore; }
			}


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

			public void SetIgnoreExceptionOnExecution<T>()
				where T : Exception
			{
				ExceptionTypesToIgnore.Add(typeof(T));
			}

			public unsafe int CompareTo(BatchOperation other)
			{
				var r = SliceEqualityComparer.Instance.Compare(Key, other.Key);
				if (r != 0)
					return r;
				if (valSlice != null)
				{
					if (other.valSlice == null)
						return -1;
					return valSlice.Compare(other.valSlice, NativeMethods.memcmp);
				}
				else if (other.valSlice != null)
				{
					return 1;
				}
				return 0;
			}
		}

		public enum BatchOperationType
		{
			None = 0,
			Add = 1,
			Delete = 2,
			MultiAdd = 3,
			MultiDelete = 4,
			Increment = 5
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