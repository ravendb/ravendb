namespace Voron.Impl
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.IO.MemoryMappedFiles;
	using System.Linq;
	using System.Runtime.InteropServices;

	public unsafe class MemoryMapPager : AbstractPager
	{
		private readonly FlushMode _flushMode;
		private readonly FileStream _fileStream;
		private int numberOfFlushes;

		[DllImport("kernel32.dll", SetLastError = true)]
		[return: MarshalAs(UnmanagedType.Bool)]
		extern static bool FlushViewOfFile(byte* lpBaseAddress, IntPtr dwNumberOfBytesToFlush);

		public MemoryMapPager(string file, FlushMode flushMode = FlushMode.Full)
		{
			_flushMode = flushMode;
			var fileInfo = new FileInfo(file);
			var hasData = fileInfo.Exists == false || fileInfo.Length == 0;
			_fileStream = fileInfo.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
			if (hasData)
			{
				NumberOfAllocatedPages = 0;
			}
			else
			{
				NumberOfAllocatedPages = fileInfo.Length / PageSize;
				PagerState.Release();
				PagerState = CreateNewPagerState();
			}
		}



		public override void AllocateMorePages(Transaction tx, long newLength)
		{
			if (newLength < _fileStream.Length)
				throw new ArgumentException("Cannot set the legnth to less than the current length");

			if (newLength == _fileStream.Length)
				return;

			// need to allocate memory again
			_fileStream.SetLength(newLength);
			PagerState.Release(); // when the last transaction using this is over, will dispose it
			PagerState newPager = CreateNewPagerState();

			if (tx != null) // we only pass null during startup, and we don't need it there
			{
				newPager.AddRef(); // one for the current transaction
				tx.AddPagerState(newPager);
			}

			PagerState = newPager;
			NumberOfAllocatedPages = newPager.Accessor.Capacity / PageSize;
		}

		private PagerState CreateNewPagerState()
		{
			var mmf = MemoryMappedFile.CreateFromFile(_fileStream, Guid.NewGuid().ToString(), _fileStream.Length,
													  MemoryMappedFileAccess.ReadWrite, null, HandleInheritability.None, true);
			var accessor = mmf.CreateViewAccessor();
			byte* p = null;
			accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref p);

			var newPager = new PagerState
			{
				Accessor = accessor,
				File = mmf,
				Base = p
			};
			newPager.AddRef(); // one for the pager
			return newPager;
		}


		public override void Flush(List<long> sortedPagesToFlush)
		{
			if (_flushMode == FlushMode.None || sortedPagesToFlush.Count == 0)
				return;

			var pageRangesToFlush = GetPageRangesToFlush(sortedPagesToFlush).ToList();
			foreach (var tuple in pageRangesToFlush)
			{
				FlushPages(tuple.Item1, tuple.Item2);
			}
			//numberOfFlushes++;
			//if (
			//    new[] { 1, 2, 3, 27, 28, 29, 1153, 1154, 1155, 4441, 4442, 4443, 7707, 7708, 7709, 9069, 9070, 9071 }.Contains(
			//        numberOfFlushes) == false)
			//    return;

			//var pages = sortedPagesToFlush.Select(Get).ToList();

			//Console.WriteLine("Flush {3,6:#,#} with {0,3:#,#} pages - {1,3:#,#} kb writes and {2,3} seeks ({4,3:#,#;;0} leaves, {5,3:#,#;;0} branches, {6,3:#,#;;0} overflows)",
			//                sortedPagesToFlush.Count,
			//                  (sortedPagesToFlush.Count * PageSize) / 1024,
			//                  pageRangesToFlush.Count,
			//                  numberOfFlushes,
			//                  pages.Count(x => x.IsLeaf),
			//                  pages.Count(x => x.IsBranch),
			//                  pages.Count(x => x.IsOverflow)
			//                  );
		}

		public override void Flush(long headerPageId)
		{
			FlushPages(headerPageId, 1);
		}

		public override void Sync()
		{
			if (_flushMode == FlushMode.Full)
				_fileStream.Flush(true);
		}

		private IEnumerable<Tuple<long, long>> GetPageRangesToFlush(List<long> sortedPagesToFlush)
		{
			// here we try to optimize the amount of work we do, we will only 
			// flush the actual dirty pages, and we will do so in sequential order
			// ideally, this will save the OS the trouble of actually having to flush the 
			// entire range
			long start = sortedPagesToFlush[0];
			long count = 1;
			for (int i = 1; i < sortedPagesToFlush.Count; i++)
			{
				var difference = sortedPagesToFlush[i] - sortedPagesToFlush[i - 1];
				// if the difference between them is not _too_ big, we will just merge it into a single call
				// we are trying to minimize both the size of the range that we flush AND the number of times
				// we call flush, so we need to balance those needs.
				if (difference < 32)
				{
					count += difference;
					continue;
				}
				yield return Tuple.Create(start, count);
				start = sortedPagesToFlush[i];
				count = 1;
			}
			yield return Tuple.Create(start, count);
		}


		private void FlushPages(long startPage, long count)
		{
			long numberOfBytesToFlush = count * PageSize;
			long start = startPage * PageSize;
			FlushViewOfFile(PagerState.Base + start, new IntPtr(numberOfBytesToFlush));
		}

		public override void EnsureEnoughSpace(Transaction tx, int len)
		{
			// no need to do anything
		}

		public override void Dispose()
		{
			base.Dispose();
			if (PagerState != null)
			{
				PagerState.Release();
				PagerState = null;
			}
			_fileStream.Dispose();
		}
	}
}
