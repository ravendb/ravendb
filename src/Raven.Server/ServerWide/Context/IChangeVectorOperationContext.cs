using System.Collections.Generic;
using Raven.Server.Utils;

namespace Raven.Server.ServerWide.Context;

public interface IChangeVectorOperationContext
{
    ChangeVector GetChangeVector(string changeVector, bool throwOnRecursion = false);

    ChangeVector GetChangeVector(string version, string order);
}

// FOR TESTING ONLY
public class NoChangeVectorContext : IChangeVectorOperationContext
{
    public static NoChangeVectorContext Instance = new NoChangeVectorContext();

    public ChangeVector GetChangeVector(string changeVector, bool throwOnRecursion = false) => new ChangeVector(changeVector, throwOnRecursion, this);

    public ChangeVector GetChangeVector(string version, string order)
    {
        return new ChangeVector(new ChangeVector(version, throwOnRecursion: true, this), 
            new ChangeVector(version, throwOnRecursion: true, this));
    }
}
