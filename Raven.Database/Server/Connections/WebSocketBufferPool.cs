// -----------------------------------------------------------------------
//  <copyright file="WebSocketsPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Raven.Database.Server.Connections
{
	public class WebSocketBufferPool
	{
		private const int TakeRetries = 128;
		private const int BufferSize = 1024;
		private readonly int numberOfBuffersToAllocate = 128;
		private readonly ConcurrentStack<ArraySegment<byte>> buffersOnGen2OrLoh = new ConcurrentStack<ArraySegment<byte>>();
		private readonly Version dotNetVersion = Environment.Version;
		private readonly Version dotNetVersion_4_5_2 = Version.Parse("4.0.30319.34000");

		public WebSocketBufferPool(int webSocketPoolSizeInBytes)
		{
			numberOfBuffersToAllocate = webSocketPoolSizeInBytes/BufferSize;

			AllocateBuffers();
		}

		private void AllocateBuffers()
		{
			if (dotNetVersion.CompareTo(dotNetVersion_4_5_2) > 0) // >= .NET 4.5.2
			{
				var bytes = new byte[numberOfBuffersToAllocate * BufferSize];

				for (var i = 0; i < numberOfBuffersToAllocate; i++)
				{
					var buffer = new ArraySegment<byte>(bytes, i * BufferSize, BufferSize);
					buffersOnGen2OrLoh.Push(buffer);
				}
			}
			else
			{
				// there was a bug (fixed in .NET 4.5.2) which doesn't allow us to specify non-zero offset in ArraySegment
				// https://connect.microsoft.com/VisualStudio/feedback/details/812310/bug-websockets-can-only-use-0-offset-internal-buffer

				var newBuffers = new List<byte[]>();

				for (var i = 0; i < numberOfBuffersToAllocate; i++)
				{
					newBuffers.Add(new byte[BufferSize]);
				}

				while (true)
				{
					// force to move to Gen2
					for (int i = 0; i < GC.MaxGeneration; i++)
					{
						GC.Collect(i, GCCollectionMode.Forced, true);
					}
					bool atLeastOneSentToGen2 = false;
					foreach (var buffer in newBuffers)
					{
						if (GC.GetGeneration(buffer) == GC.MaxGeneration)
						{
							atLeastOneSentToGen2 = true;
							buffersOnGen2OrLoh.Push(new ArraySegment<byte>(buffer));
						}
					}
					if (atLeastOneSentToGen2)
						break;
				}
			}
		}

		public ArraySegment<byte> TakeBuffer()
		{
			var retries = 0;
			// ReSharper disable once TooWideLocalVariableScope
			ArraySegment<byte> buffer;
				
			while (retries++ < TakeRetries)
			{
				if (buffersOnGen2OrLoh.TryPop(out buffer))
					return buffer;
				
				AllocateBuffers();
			}

			throw new InvalidOperationException("Unable to take a web socket buffer");
		}

		public void ReturnBuffer(ArraySegment<byte> buffer)
		{
			buffersOnGen2OrLoh.Push(buffer);
		}
	}
}