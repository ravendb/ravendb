// -----------------------------------------------------------------------
//  <copyright file="WebSocketsPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using Raven.Abstractions.Util;

namespace Raven.Database.Server.Connections
{
    public class WebSocketBufferPool
    {
        private const int BufferSize = 1024;
        public static WebSocketBufferPool Instance;
        private PinnableBufferCache pinnableBufferCache;

        private WebSocketBufferPool(int webSocketPoolSizeInBytes)
        {
            pinnableBufferCache = new PinnableBufferCache("websocketbufferpool", BufferSize);
        }

        public static void Initialize(int webSocketPoolSizeInBytes)
        {
            if (Instance != null)
                return;

            Instance = new WebSocketBufferPool(webSocketPoolSizeInBytes);
        }
        
        public ArraySegment<byte> TakeBuffer()
        {
            return new ArraySegment<byte>(pinnableBufferCache.AllocateBuffer());
        }

        public void ReturnBuffer(ArraySegment<byte> buffer)
        {
            pinnableBufferCache.FreeBuffer(buffer.Array);
        }
    }
}
