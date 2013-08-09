using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Nevar.Trees;

namespace Nevar.Impl
{
	/// <summary>
	/// Free space is store in Voron in one of two ways:
	/// 
	/// - Free Space by transaction (all the pages freed in a particular transaction)
	/// - Free Space by section
	/// 
	/// Sections are 4MB ranges of the file. All the free space in a section is gathered up
	/// when a transaction is freed and written to the section.
	/// 
	/// In addition to that, there is also the _current_ section. All the free space allocations are
	/// done from the current section. 
	/// 
	/// When all the free space is gone from the current section, we select the next appropriate section.
	/// This would be the next section of the file (forward wrapping order) that has enough free pages.
	/// 
	/// Ideally, we want to re-use the section for multiple transactions, so we need to find a large one.
	/// We do that by finding a section with a minimum applicable amount, then search the next 256 (about 1GB) 
	/// sections for a section with higher count of free pages.
	/// 
	/// The structure of a transaction in storage is:
	/// 
	/// key - tx/* (always increasing)
	/// value - int64 - tx id, int64* - freed pages
	/// 
	/// The structure of a section in storage is:
	/// 
	/// key = sect/[section id] (lexically sorted)
	/// value - int64 id, int64* - freed pages
	/// 
	/// </summary>
	public unsafe class FreeSpaceRepository
	{
		private readonly StorageEnvironment _env;
		private int _lastTransactionPageUsage;

		public class Section
		{
			public Slice Key;
			public long Id;
			public List<long> Pages = new List<long>();
		}

		public class FreedTransaction
		{
			public Slice Key;
			public long Id;
			public List<long> Pages;
		}

		private readonly LinkedList<FreedTransaction> _freedTransactions = new LinkedList<FreedTransaction>();

		private Section _current;
		private Slice _currentKey;
		private bool _currentChanged;
		private readonly Slice _sectionsPrefix = "sect/";
		private readonly Slice _txPrefix = "tx/";
		private int _minimumFreePagesInSection;
		private bool _minimumFreePagesInSectionSet;

		public FreeSpaceRepository(StorageEnvironment env)
		{
			_env = env;
		}

		public Page TryAllocateFromFreeSpace(Transaction tx, int num)
		{
			if (_env.FreeSpaceRoot == null)
				return null;// this can happen the first time FreeSpaceRoot tree is created

			if (_current == null || _current.Pages.Count == 0)
			{
				var old = _current;
				try
				{
					if (SetupNextSection(tx, _currentKey) == false)
					{
						_current = null;
						_currentKey = null;
						_currentChanged = false;
						return null;
					}
				}
				finally
				{
					DeleteSection(tx, old);
				}
			}

			Debug.Assert(_current != null);
			Debug.Assert(_current.Pages.Count > 0);
			var index = _current.Pages.Count - 1;//end
			var page = _current.Pages[index];
			_current.Pages.RemoveAt(index);

			_currentChanged = true;

			var newPage = tx.Pager.Get(tx, page);
			newPage.PageNumber = page;
			return newPage;
		}

		private void DeleteSection(Transaction tx, Section old)
		{
			if (old == null)
				return;
			_env.FreeSpaceRoot.Delete(tx, old.Key);
		}

		public long GetFreePageCount()
		{
			long freePages = 0;
			using (var tx = _env.NewTransaction(TransactionFlags.Read))
			using (var it = _env.FreeSpaceRoot.Iterate(tx))
			{
				var key = new Slice(SliceOptions.Key);
				if (it.Seek(Slice.BeforeAllKeys) == false)
				{
					return 0;
				}
				do
				{
					key.Set(it.Current);

					if (key.StartsWith(_sectionsPrefix, _env.SliceComparer) || key.StartsWith(_txPrefix, _env.SliceComparer))
					{
						var dataSize = NodeHeader.GetDataSize(tx, it.Current);
						freePages += dataSize/sizeof (long) - 1;
					}
					else
					{
						Debug.Assert(false, "invalid key in free space tree: " + key);
					}

				} while (it.MoveNext());
			}
			return freePages;

		}

		private bool SetupNextSection(Transaction tx, Slice key)
		{
			UpdateSections(tx, _env.OldestTransaction);
			int currentMax = 0;
			NodeHeader* current = null;
			bool hasMatch = key != null &&
				TryFindSection(tx, key, key, null, ref current, ref currentMax);
			if (hasMatch == false) // wrap to the beginning 
			{
				if (TryFindSection(tx, key, _sectionsPrefix, key, ref current, ref currentMax) == false)
					return false;
			}
			Debug.Assert(current != null);

			_current = new Section
				{
					Key = new Slice(current).Clone(),
					Pages = new List<long>(),
				};

			using (var stream = NodeHeader.Stream(tx, current))
			using (var reader = new BinaryReader(stream))
			{
				_current.Id = reader.ReadInt64();
				for (var i = 1; i < currentMax; i++)
				{
					_current.Pages.Add(reader.ReadInt64());
				}
			}

			_currentKey = _current.Key;
			_currentChanged = false;

			return true;
		}

		private bool TryFindSection(Transaction tx, Slice currentKey, Slice start, Slice end, ref NodeHeader* current, ref int currentMax)
		{
			int minFreeSpace = _minimumFreePagesInSectionSet ?
				_minimumFreePagesInSection :
				Math.Min(256, (_lastTransactionPageUsage * 3) / 2);

			using (var it = _env.FreeSpaceRoot.Iterate(tx))
			{
				it.RequiredPrefix = _sectionsPrefix;
				it.MaxKey = end;
				if (it.Seek(start) == false)
					return false;

				int triesAfterFindingSuitable = 256;
				do
				{
					if (current != null)
						triesAfterFindingSuitable--;

					if (currentKey != null)
					{
						if (_currentKey.Compare(new Slice(it.Current), _env.SliceComparer) == 0)
							continue; // skip current one
					}

					var nodeSize = NodeHeader.GetDataSize(tx, it.Current);
					var numberOfFreePages = nodeSize / sizeof(long);
					if (numberOfFreePages < minFreeSpace || numberOfFreePages < currentMax)
						continue;

					current = it.Current;
					currentMax = numberOfFreePages;
				} while (it.MoveNext() && triesAfterFindingSuitable >= 0);
			}
			return current != null;
		}

		public void UpdateSections(Transaction tx, long oldestTx)
		{
			var sections = new Dictionary<long, Section>();
			if (_current != null)
				sections[_current.Id] = _current;

			while (_freedTransactions.First != null && _freedTransactions.First.Value.Id < oldestTx)
			{
				var val = _freedTransactions.First.Value;
				_freedTransactions.RemoveFirst();

				foreach (var page in val.Pages)
				{
					var sectionId = page / 1024;
					Section section;
					if (sections.TryGetValue(sectionId, out section) == false)
					{
						sections[sectionId] = section = LoadSection(tx, sectionId);
					}

					if (section == _current)
						_currentChanged = true;

					section.Pages.Add(page);
				}
			}

			using (var ms = new MemoryStream())
			{
				foreach (var section in sections.Values)
				{
					if (_currentChanged == false && section == _current)
						continue; // we can skip this

					section.Pages.Sort((x, y) => x.CompareTo(y) * -1); // desc sort
					ms.SetLength(0);

					using (var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
					{
						writer.Write(section.Id);
						foreach (var page in section.Pages)
						{
							writer.Write(page);
						}
					}

					ms.Position = 0;
					_env.FreeSpaceRoot.Add(tx, section.Key, ms);
				}
			}

			_currentChanged = false;
		}

		private Section LoadSection(Transaction tx, long sectionId)
		{
			Slice key = string.Format("sect/{0:0000000000000000000}", sectionId);

			var section = new Section
			{
				Key = key,
				Pages = new List<long>(),
				Id = sectionId
			};
			using (var stream = _env.FreeSpaceRoot.Read(tx, key))
			{
				if (stream == null)
					return section;

				using (var reader = new BinaryReader(stream))
				{
					var savedSectionId = reader.ReadInt64();
					Debug.Assert(savedSectionId == sectionId);

					var pageCount = stream.Length / sizeof(long) - 1;
					for (int i = 0; i < pageCount; i++)
					{
						section.Pages.Add(reader.ReadInt64());
					}
				}
				return section;
			}
		}

		public void Restore(Transaction tx)
		{
			// read all the freed transactions to memory, we expect this
			// number to be pretty small, so we don't worry about holding it all 
			// in memory
			using (var it = _env.FreeSpaceRoot.Iterate(tx))
			{
				it.RequiredPrefix = _txPrefix;

				if (it.Seek(_txPrefix) == false)
					return;

				do
				{
					var freedTransaction = new FreedTransaction
						{
							Key = new Slice(it.Current).Clone(),
							Pages = new List<long>()
						};

					var size = NodeHeader.GetDataSize(tx, it.Current) / sizeof(long);

					Debug.Assert(size > 1);


					using (var stream = NodeHeader.Stream(tx, it.Current))
					using (var reader = new BinaryReader(stream))
					{
						freedTransaction.Id = reader.ReadInt64();
						for (var i = 1; i < size; i++)
						{
							freedTransaction.Pages.Add(reader.ReadInt64());
						}
					}

					_freedTransactions.AddLast(freedTransaction);
				} while (it.MoveNext());
			}
		}

		public void LastTransactionPageUsage(int pages)
		{
			if (_lastTransactionPageUsage < pages)
			{
				// to register a drop, we need to see a 25% reduction in the size
				if (pages - _lastTransactionPageUsage < _lastTransactionPageUsage / 4)
					return;
			}

			_lastTransactionPageUsage = pages;
		}

		public void RegisterFreePages(Slice key, long id, List<long> freedPages)
		{
			_freedTransactions.AddLast(new FreedTransaction
				{
					Key = key,
					Id = id,
					Pages = new List<long>(freedPages)
				});
		}

		public int MinimumFreePagesInSection
		{
			get { return _minimumFreePagesInSection; }
			set
			{
				_minimumFreePagesInSectionSet = true;
				_minimumFreePagesInSection = value;
			}
		}
	}
}
