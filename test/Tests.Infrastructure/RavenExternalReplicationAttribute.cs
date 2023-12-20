using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Tests.Infrastructure;

public class RavenExternalReplicationAttribute : RavenDataAttributeBase
{
    private readonly RavenDatabaseMode _source;
    private readonly RavenDatabaseMode _destination;
    private object[] _data;

    public RavenExternalReplicationAttribute(
        RavenDatabaseMode source,
        RavenDatabaseMode destination
    )
    {
        _source = source;
        _destination = destination;
    }

    public RavenExternalReplicationAttribute(
        RavenDatabaseMode source,
        RavenDatabaseMode destination,
        params object[] data
    ) : this(source, destination)
    {
        _data = data;
    }

    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        foreach (var (dstDatabaseMode, dstOptions) in RavenDataAttribute.GetOptions(_destination))
        {
            foreach (var (srcDatabaseMode, srcOptions) in RavenDataAttribute.GetOptions(_source))
            {
                using (SkipIfNeeded(dstDatabaseMode))
                using (SkipIfNeeded(srcDatabaseMode))
                {
                    if (_data == null || _data.Length == 0)
                    {
                        yield return new object[] { srcOptions, dstOptions };
                        continue;
                    }

                    yield return new object[] { srcOptions, dstOptions }.Concat(_data).ToArray();
                }
            }
        }
    }
}
