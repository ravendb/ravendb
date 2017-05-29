using System;
using System.Collections.Generic;
using System.Text;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public interface IDynamicJsonValueConvertible
    {
        DynamicJsonValue ToJson();
    }
}
