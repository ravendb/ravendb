using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Impl.Synchronization
{
	public class EtagSynchronizer
	{
		private readonly object locker = new object();

		private Etag currentEtag;

		private Etag synchronizationEtag;

		private readonly EtagSynchronizerType type;

		private readonly ITransactionalStorage transactionalStorage;

		public EtagSynchronizer(EtagSynchronizerType type, ITransactionalStorage transactionalStorage)
		{
			this.type = type;
			this.transactionalStorage = transactionalStorage;

			LoadSynchronizationState();
		}

		public void UpdateSynchronizationState(Etag lowestEtag)
		{
			lock (locker)
			{
				if (UpdateSynchronizationStateInternal(lowestEtag))
					PersistSynchronizationState();
			}
		}

		public Etag CalculateSynchronizationEtag(Etag etag, Etag lastProcessedEtag)
		{
			if (etag == null)
			{
				if (lastProcessedEtag != null)
				{
					lock (locker)
					{
						if (currentEtag == null && lastProcessedEtag.CompareTo(synchronizationEtag) != 0)
						{
							synchronizationEtag = lastProcessedEtag;
							PersistSynchronizationState();
						}
					}

					return lastProcessedEtag;
				}

				return Etag.Empty;
			}

			if (lastProcessedEtag == null)
				return Etag.Empty;

			if (etag.CompareTo(lastProcessedEtag) < 0)
				return EtagUtil.Increment(etag, -1);

			return lastProcessedEtag;
		}

		public Etag GetSynchronizationEtag()
		{
			lock (locker)
			{
				var etag = currentEtag;
				if (etag != null)
				{
					PersistSynchronizationState();
					synchronizationEtag = currentEtag;
					currentEtag = null;
				}

				return etag;
			}
		}

		private bool UpdateSynchronizationStateInternal(Etag lowestEtag)
		{
			if (currentEtag == null || lowestEtag.CompareTo(currentEtag) < 0)
			{
				currentEtag = lowestEtag;
			}

			return lowestEtag.CompareTo(synchronizationEtag) < 0;
		}

		private void PersistSynchronizationState()
		{
			using (transactionalStorage.DisableBatchNesting())
			{
				transactionalStorage.Batch(
					actions => actions.Lists.Set("Raven/Etag/Synchronization", type.ToString(), RavenJObject.FromObject(new
					{
						etag = GetEtagForPersistance()
					}), UuidType.EtagSynchronization));
			}	
		}

		private void LoadSynchronizationState()
		{
			transactionalStorage.Batch(actions =>
			{
				var state = actions.Lists.Read("Raven/Etag/Synchronization", type.ToString());
				if (state == null)
				{
					currentEtag = null;
					synchronizationEtag = Etag.Empty;
					return;
				}

				var etag = Etag.Parse(state.Data.Value<string>("etag"));
				currentEtag = etag;
				synchronizationEtag = etag;
			});
		}

		private Etag GetEtagForPersistance()
		{
			var result = synchronizationEtag;
			if (currentEtag != null)
			{
				result = currentEtag.CompareTo(synchronizationEtag) < 0
					         ? currentEtag
					         : synchronizationEtag;
			}
			return result ?? Etag.Empty;
		}
	}
}