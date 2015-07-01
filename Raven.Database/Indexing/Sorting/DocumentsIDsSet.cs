using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Database.Indexing.Sorting
{
	/// <summary>
	/// Used to calculate existance of fields in an index, where the field doesn't exists
	/// for most of the documents.
	/// 
	/// It has 1Kb overhead at all times, with a worst case scenario, takes NumberOfDocuments bits + 1Kb.
	/// 
	/// Given a 256K documents in an index, the worst case would be 33Kb set.
	/// 
	/// A more common scenrio, where we have much fewer items in the set than the documents (let us say, 4K out of 256K), we 
	/// would take 1Kb + 2Kb only.
	/// </summary>
	public class SparseDocumentIdSet
	{
		private const int _numOfBitArrays = 128;
		private readonly BitArray[] _bitArrays = new BitArray[_numOfBitArrays];
		private readonly int _bitArraySize;

		public SparseDocumentIdSet(int size)
		{
			_bitArraySize = size / _numOfBitArrays;
		}

		public void Set(int docId)
		{
			var idx = docId / _bitArraySize;
			if (_bitArrays[idx] == null)
				_bitArrays[idx] = new BitArray(_bitArraySize);

			_bitArrays[idx].Set(docId % _bitArraySize, true);
		}

		public bool Contains(int docId)
		{
			var idx = docId / _bitArraySize;
			if (_bitArrays[idx] == null)
				return false;
			return _bitArrays[idx].Get(idx);

		}
	}

	public class DocumentsIDsSetBuilder
	{
		private readonly int _size;
		private List<int> _small = new List<int>(32);
		private SparseDocumentIdSet _large;
		public DocumentsIDsSetBuilder(int size)
		{
			this._size = size;
		}

		public void Set(int docId)
		{
			if (_large != null)
				_large.Set(docId);
			if (_small.Count + 1 > 256)
			{
				_large = new SparseDocumentIdSet(_size);
				foreach (var doc in _small)
				{
					_large.Set(doc);
				}
				_large.Set(docId);
				return;
			}
			_small.Add(docId);
		}

		public Predicate<int> Build()
		{
			if (_large != null)
				return _large.Contains;
			_small.Sort();
			return i => _small.BinarySearch(i) >= 0;
		}
	}
}
