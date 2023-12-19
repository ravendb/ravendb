using System;
using System.Linq.Expressions;
using Sparrow.Json.Parsing;

namespace Raven.Server.Config;

public class ModifierConfigFileHelper
{
    public static void SetOrRemoveIfDefault<T, T1>(DynamicJsonValue jsonValue, T1 value, Expression<Func<RavenConfiguration, T>> getKey)
    {
        var defaultValue = (T1)RavenConfiguration.GetDefaultValue(getKey);
        if (value.Equals(defaultValue))
        {
            jsonValue.Remove(RavenConfiguration.GetKey(getKey));
        }
        else
        {
            jsonValue[RavenConfiguration.GetKey(getKey)] = value;
        }
    }
}
