using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Config;

public class SettingsJsonModifier : JsonConfigFileModifier
{
    private SettingsJsonModifier(JsonOperationContext context, string path, bool overwriteWholeFile = false) 
        : base(context, path, overwriteWholeFile)
    {
    }

    public new static SettingsJsonModifier Create(JsonOperationContext context, string path, bool reset = false)
    {
        var obj = new SettingsJsonModifier(context, path, reset);
        obj.Initialize();
        return obj;
    }

    protected override void Validate(string path)
    {
        RavenConfiguration.CreateForTesting("for-validattion", ResourceType.Server, path).Initialize();
    }

    public void SetOrRemoveIfDefault<T, T1>(T1 value, Expression<Func<RavenConfiguration, T>> getKey)
    {
        var key = RavenConfiguration.GetKey(getKey);
        if (CheckIfDefault(value, RavenConfiguration.GetDefaultValue(getKey)))
        {
            //We don't want to remove the configuration entry if it was set explicitly to default 
            if (IsOriginalValue(key, value) == false)
                Modifications.Remove(key);
        }
        else
        {
            Modifications[key] = value;
        }
    }
    
    public void CollectionSetOrRemoveIfDefault<T, T1>(IEnumerable<T1> value, Expression<Func<RavenConfiguration, T>> getKey)
    {
        var listValue = string.Join(';', value);
        SetOrRemoveIfDefault(listValue, getKey);
    }
    
    private bool CheckIfDefault<T>(T value, object defaultValue)
    {
        if (value is long longValue && defaultValue is int intDefaultValue)
        {
            return CheckIfDefault(longValue, (long)intDefaultValue);
        }
        return value.Equals(defaultValue);
    }
}
