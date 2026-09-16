using System;
using Sparrow.Json;

namespace Corax.Querying.Planning;

public enum BindingSource : byte
{
    /// <summary>LiteralValue + LiteralType hold the resolved value. No runtime resolution needed.</summary>
    Literal,
    /// <summary>ParameterName → blittable lookup at execution time. May resolve to scalar or array.</summary>
    QueryParameter,
    /// <summary>DeferredExpression → evaluate at execution time (cmpxchg, now, today).</summary>
    DeferredMethod,
}

public sealed class ParameterBinding
{
    public BindingSource Source;
    public object LiteralValue;
    public ParamValueType LiteralType;
    public string ParameterName;
    
    /// <summary>
    /// The index of the parameter slot in the <see cref="PlanTemplate.ParameterSlots"/>
    /// </summary>
    public int ParameterSlot = -1;

    /// <summary>
    /// Position of the value (parameter or literal or deferred)
    /// </summary>
    public int ValueOrdinal = -1;

    /// <summary>For deferred method expressions (e.g. cmpxchg(), now(), today()).</summary>
    public Func<object, BlittableJsonReaderObject, object> DeferredExpression;
}
