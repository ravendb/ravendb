// -----------------------------------------------------------------------
//  <copyright file="WebSocketsPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;

namespace Raven.Database.Server.Connections
{
	public class WebSocketBufferPool
	{
		private const int TakeRetries = 128;
		private const int BufferSize = 1024;
		private const int NumberOfBuffersToAllocate = 2048;
		private readonly ConcurrentStack<ArraySegment<byte>> buffers = new ConcurrentStack<ArraySegment<byte>>();
		private readonly Version dotNetVersion = Environment.Version;
		private readonly Version dotNetVersion_4_5_2 = Version.Parse("4.0.30319.34000");

		public WebSocketBufferPool()
		{
			AllocateBuffers();
		}

		private void AllocateBuffers()
		{
			if (dotNetVersion.CompareTo(dotNetVersion_4_5_2) > 0) // >= .NET 4.5.2
			{
				var bytes = new byte[NumberOfBuffersToAllocate * BufferSize];

				for (var i = 0; i < NumberOfBuffersToAllocate; i++)
				{
					var buffer = new ArraySegment<byte>(bytes, i * BufferSize, BufferSize);
					buffers.Push(buffer);
				}
			}
			else
			{
				for (var i = 0; i < NumberOfBuffersToAllocate; i++)
				{
					buffers.Push(new ArraySegment<byte>(new byte[BufferSize]));
				}

				// force to move to Gen2
				GC.Collect(0, GCCollectionMode.Forced);
				GC.Collect(1, GCCollectionMode.Forced);
			}
		}

		public ArraySegment<byte> TakeBuffer()
		{
			var retries = 0;
			// ReSharper disable once TooWideLocalVariableScope
			ArraySegment<byte> buffer;
				
			while (retries++ < TakeRetries)
			{
				if (buffers.TryPop(out buffer))
					return buffer;
				
				AllocateBuffers();
			}

			throw new InvalidOperationException("Unable to take a web socket buffer");
		}

		public void ReturnBuffer(ArraySegment<byte> buffer)
		{
			buffers.Push(buffer);
		}
	}
}