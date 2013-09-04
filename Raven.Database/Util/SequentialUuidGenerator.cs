using System;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Database.Impl;

namespace Raven.Database.Util
{
	public class SequentialUuidGenerator : IUuidGenerator
	{
		private byte[] ticksAsBytes;
		private long sequentialUuidCounterDocuments;
		private long sequentialUuidCounterAttachments;
		private long sequentialUuidCounterDocumentsTransactions;
		private long sequentialUuidCounterMappedResults;
		private long sequentialUuidCounterReduceResults;
		private long sequentialUuidCounterQueue;
		private long sequentialUuidCounterTasks;
		private long sequentialUuidCounterScheduledReductions;
		private long sequentialUuidCounterIndexing;
		private long sequentialUuidEtagSynchronization;
		private long sequentialUuidDocumentReferences;

		public long EtagBase
		{
			set
			{
				ticksAsBytes = BitConverter.GetBytes(value);
				Array.Reverse(ticksAsBytes);
			}
		}

	
		public Etag CreateSequentialUuid(UuidType type)
		{
			long increment;
			switch (type)
			{
				case UuidType.Documents:
					increment = Interlocked.Increment(ref sequentialUuidCounterDocuments);
					break;
				case UuidType.Attachments:
					increment = Interlocked.Increment(ref sequentialUuidCounterAttachments);
					break;
				case UuidType.DocumentTransactions:
					increment = Interlocked.Increment(ref sequentialUuidCounterDocumentsTransactions);
					break;
				case UuidType.MappedResults:
					increment = Interlocked.Increment(ref sequentialUuidCounterMappedResults);
					break;
				case UuidType.ReduceResults:
					increment = Interlocked.Increment(ref sequentialUuidCounterReduceResults);
					break;
				case UuidType.Queue:
					increment = Interlocked.Increment(ref sequentialUuidCounterQueue);
					break;
				case UuidType.Tasks:
					increment = Interlocked.Increment(ref sequentialUuidCounterTasks);
					break;
				case UuidType.ScheduledReductions:
					increment = Interlocked.Increment(ref sequentialUuidCounterScheduledReductions);
					break;
				case UuidType.Indexing:
					increment = Interlocked.Increment(ref sequentialUuidCounterIndexing);
					break;
				case UuidType.EtagSynchronization:
					increment = Interlocked.Increment(ref sequentialUuidEtagSynchronization);
					break;
				case UuidType.DocumentReferences:
					increment = Interlocked.Increment(ref sequentialUuidDocumentReferences);
					break;
				default:
					throw new ArgumentOutOfRangeException("type", "Cannot understand: " + type);
			}

			var currentAsBytes = BitConverter.GetBytes(increment);
			Array.Reverse(currentAsBytes);
			var bytes = new byte[16];
			Array.Copy(ticksAsBytes, 0, bytes, 0, ticksAsBytes.Length);
			Array.Copy(currentAsBytes, 0, bytes, 8, currentAsBytes.Length);
			bytes[0] = (byte) type; // record the etag type, if we need it for debug later
			return Etag.Parse(bytes);
		}
	}
}