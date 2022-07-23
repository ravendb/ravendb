using Raven.Server.Utils;

namespace Raven.Server.ServerWide.Context;

public interface IChangeVectorOperationContext
{
    ChangeVector GetChangeVector(string changeVector, bool throwOnRecursion = false);

    ChangeVector GetChangeVector(string version, string order);
}
