using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.ServiceModel.Channels;

namespace Raven.Database.Util.Streams
{
	public interface IBufferPool : IDisposable
	{
		byte[] TakeBuffer(int size);
		void ReturnBuffer(byte[] buffer);
	}

	public class BufferPool : IBufferPool
	{
		private readonly BufferManager bufferManager;
#if DEBUG
		public class BufferTracker
		{
			private StackTrace stackTrace;
			public void TrackAllocation()
			{
				stackTrace = new StackTrace(true);
				GC.ReRegisterForFinalize(this);
			}

			public void Discard()
			{
				stackTrace = null;
				GC.SuppressFinalize(this);
			}

			~BufferTracker()
			{
				if (stackTrace == null)
					return;

				throw new InvalidOperationException(
					"A buffer was leaked. Initial allocation:" + Environment.NewLine + stackTrace
					);
			}
		}
		private ConditionalWeakTable<byte[], BufferTracker> trackLeakedBuffers = new ConditionalWeakTable<byte[], BufferTracker>();
#endif

		public BufferPool(long maxBufferPoolSize, int maxBufferSize)
		{
			bufferManager = BufferManager.CreateBufferManager(maxBufferPoolSize, maxBufferSize);
		}

		public void Dispose()
		{
			bufferManager.Clear();
			// note that disposing the pool before returning all of the buffers will cause a crash
		}

		public byte[] TakeBuffer(int size)
		{
			var buffer = bufferManager.TakeBuffer(size);
#if DEBUG
			trackLeakedBuffers.GetOrCreateValue(buffer).TrackAllocation();
#endif
			return buffer;
		}

		public void ReturnBuffer(byte[] buffer)
		{
#if DEBUG
			BufferTracker value;
			if (trackLeakedBuffers.TryGetValue(buffer, out value))
			{
				value.Discard();
			}
#endif
			bufferManager.ReturnBuffer(buffer);
		}
	}
}