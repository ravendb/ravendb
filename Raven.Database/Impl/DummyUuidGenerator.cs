using System;

namespace Raven.Database.Impl
{
    public class DummyUuidGenerator : IUuidGenerator
    {
        private byte cur;
        public Guid CreateSequentialUuid()
        {
            var bytes = new byte[16];
            bytes[15] += ++cur;
            return new Guid(bytes);
        }
    }
}