using System;

namespace Raven.Database.Impl
{
    public interface IUuidGenerator
    {
        Guid CreateSequentialUuid();
    }
}