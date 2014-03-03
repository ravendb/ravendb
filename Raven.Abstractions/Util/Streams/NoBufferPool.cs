// -----------------------------------------------------------------------
//  <copyright file="NoBufferPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Abstractions.Util.Streams
{
    public class NoBufferPool : IBufferPool
    {
        public void Dispose()
        {
        }

        public byte[] TakeBuffer(int size)
        {
            return new byte[size];
        }

        public void ReturnBuffer(byte[] buffer)
        {
        }
    }
}