using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Nevar.Debugging;
using Nevar.Impl;

namespace Nevar.Benchmark
{
	unsafe class Program
	{
		private static HashSet<long> _randomNumbers;
		public const int Count = 1000 * 1000;
		private const string _path = @"bench.data";

		static void Main()
		{
			InitRandomNumbers();

			Time("fill rnd none", sw => FillRandomOneTransaction(sw, FlushMode.None));
			Time("fill rnd buff", sw => FillRandomOneTransaction(sw, FlushMode.Buffers));
			Time("fill rnd sync", sw => FillRandomOneTransaction(sw, FlushMode.Full));

			Time("fill seq none 10,000 tx", sw => FillRandomMultipleTransaction(sw, FlushMode.None, 10 * 1000));
			Time("fill seq buff 10,000 tx", sw => FillRandomMultipleTransaction(sw, FlushMode.Buffers, 10 * 1000));
			Time("fill seq sync 10,000 tx", sw => FillRandomMultipleTransaction(sw, FlushMode.Full, 10 * 1000));
			
			Time("fill seq none", sw => FillSeqOneTransaction(sw, FlushMode.None));
			Time("fill seq buff", sw => FillSeqOneTransaction(sw, FlushMode.Buffers));
			Time("fill seq sync", sw => FillSeqOneTransaction(sw, FlushMode.Full));

			Time("fill seq none 10,000 tx", sw => FillSeqMultipleTransaction(sw, FlushMode.None, 10 * 1000));
			Time("fill seq buff 10,000 tx", sw => FillSeqMultipleTransaction(sw, FlushMode.Buffers, 10 * 1000));
			Time("fill seq sync 10,000 tx", sw => FillSeqMultipleTransaction(sw, FlushMode.Full, 10 * 1000));

			Time("read seq", ReadOneTransaction, delete: false);
			Time("read parallel 1", sw => ReadOneTransaction_Parallel(sw, 1), delete: false);
			Time("read parallel 2", sw => ReadOneTransaction_Parallel(sw, 2), delete: false);
			Time("read parallel 4", sw => ReadOneTransaction_Parallel(sw, 4), delete: false);
			Time("read parallel 8", sw => ReadOneTransaction_Parallel(sw, 8), delete: false);
			Time("read parallel 16", sw => ReadOneTransaction_Parallel(sw, 16), delete: false);
		}

		private static void InitRandomNumbers()
		{
			var random = new Random();
			_randomNumbers = new HashSet<long>();
			while (_randomNumbers.Count < Count)
			{
				_randomNumbers.Add(random.Next(0, int.MaxValue));
			}
		}

		private static void Time(string name, Action<Stopwatch> action, bool delete = true)
		{
			if (File.Exists(_path) && delete)
				File.Delete(_path);
			var sp = new Stopwatch();
			action(sp);

			Console.WriteLine("{0} took\t{1:#,#} ms \t {2:#,#} ops / sec", name, sp.ElapsedMilliseconds, Count / sp.Elapsed.TotalSeconds);
		}

		private static void FillRandomOneTransaction(Stopwatch sw, FlushMode flushMode)
		{
			using (var env = new StorageEnvironment(new MemoryMapPager(_path, flushMode)))
			{
				var value = new byte[100];
				new Random().NextBytes(value);
				var ms = new MemoryStream(value);
				

				sw.Start();
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					foreach (var l in _randomNumbers)
					{
						ms.Position = 0;
						env.Root.Add(tx, l.ToString("0000000000000000"), ms);
					}

					tx.Commit();
				}
				sw.Stop();
			}
		}


		private static void FillSeqOneTransaction(Stopwatch sw, FlushMode flushMode)
		{
			using (var env = new StorageEnvironment(new MemoryMapPager(_path, flushMode)))
			{
				var value = new byte[100];
				new Random().NextBytes(value);
				var ms = new MemoryStream(value);
				
				sw.Start();
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (long i = 0; i < Count; i++)
					{
						ms.Position = 0;
						env.Root.Add(tx, i.ToString("0000000000000000"), ms);
					}

					tx.Commit();
				}
				sw.Stop();
			}
		}

		private static void FillRandomMultipleTransaction(Stopwatch sw, FlushMode flushMode, int parts)
		{
			using (var env = new StorageEnvironment(new MemoryMapPager(_path, flushMode)))
			{
				var value = new byte[100];
				new Random().NextBytes(value);
				var ms = new MemoryStream(value);

				sw.Start();
				var enumerator = _randomNumbers.GetEnumerator();
				for (int x = 0; x < parts; x++)
				{
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						for (long i = 0; i < Count / parts; i++)
						{
							ms.Position = 0;
							enumerator.MoveNext();
							env.Root.Add(tx, (enumerator.Current).ToString("0000000000000000"), ms);
						}

						tx.Commit();
					}
				}
				sw.Stop();
			}
		}

		private static void FillSeqMultipleTransaction(Stopwatch sw,FlushMode flushMode, int parts)
		{
			using (var env = new StorageEnvironment(new MemoryMapPager(_path, flushMode)))
			{
				var value = new byte[100];
				new Random().NextBytes(value);
				var ms = new MemoryStream(value);

				sw.Start();
				int counter = 0;
				for (int x = 0; x < parts; x++)
				{
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						for (long i = 0; i < Count / parts; i++)
						{
							ms.Position = 0;
							env.Root.Add(tx, (counter++).ToString("0000000000000000"), ms);
						}

						tx.Commit();
					}
				}
				sw.Stop();
			}
		}


		private static void ReadOneTransaction_Parallel(Stopwatch sw, int parts)
		{
			using (var env = new StorageEnvironment(new MemoryMapPager(_path)))
			{
				var countdownEvent = new CountdownEvent(parts);
				sw.Start();
				for (int i = 0; i < parts; i++)
				{
					var currentBase = i;
					ThreadPool.QueueUserWorkItem(state =>
					{
						using (var tx = env.NewTransaction(TransactionFlags.Read))
						{
							var ms = new MemoryStream(100);
							for (int j = 0; j < Count / parts; j++)
							{
								var current = j * currentBase;
								var key = current.ToString("0000000000000000");
								using (var stream = env.Root.Read(tx, key))
								{
									ms.Position = 0;
									stream.CopyTo(ms);
								}
							}

							tx.Commit();
						}

						countdownEvent.Signal();
					});
				}
				countdownEvent.Wait();
				sw.Stop();
			}
		}

		private static void ReadOneTransaction(Stopwatch sw)
		{
			using (var env = new StorageEnvironment(new MemoryMapPager(_path)))
			{
				sw.Start();
				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					var ms = new MemoryStream(100);
					for (int i = 0; i < Count; i++)
					{
						var key = i.ToString("0000000000000000");
						using (var stream = env.Root.Read(tx, key))
						{
							ms.Position = 0;
							stream.CopyTo(ms);
						}
					}

					tx.Commit();
				}
				sw.Stop();
			}
		}



	}
}
