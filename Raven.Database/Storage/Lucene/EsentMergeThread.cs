using System;
using Lucene.Net.Index;

namespace Raven.Database.Storage
{
	public class EsentMergeConcurrentMergeScheduler : ConcurrentMergeScheduler
	{
		private readonly TransactionalStorage transactionalStorage;

		public EsentMergeConcurrentMergeScheduler(TransactionalStorage transactionalStorage)
		{
			this.transactionalStorage = transactionalStorage;
		}

		protected override MergeThread GetMergeThread(IndexWriter writer, MergePolicy.OneMerge merge)
		{
			lock (this)
			{
				MergeThread thread = new EsentMergeThread(this, writer, merge, transactionalStorage);
				thread.SetThreadPriority(GetMergeThreadPriority());
				thread.IsBackground = true;
				thread.Name = "Lucene Merge Thread #" + mergeThreadCount++;
				return thread;
			}
		}

		public class EsentMergeThread : MergeThread
		{
			private readonly TransactionalStorage storage;

			public EsentMergeThread(EsentMergeConcurrentMergeScheduler esentMergeConcurrentMergeScheduler, IndexWriter indexWriter, MergePolicy.OneMerge merge, TransactionalStorage storage)
				: base(esentMergeConcurrentMergeScheduler, indexWriter, merge)
			{
				this.storage = storage;
			}

			public override void Run()
			{
				storage.Batch(actions => BaseRun());
			}

			private void BaseRun()
			{
				base.Run();
			}
		}
	}
}