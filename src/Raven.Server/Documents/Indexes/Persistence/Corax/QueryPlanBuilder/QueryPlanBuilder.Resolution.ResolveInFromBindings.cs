using System;
using System.Collections.Generic;
using Corax.Querying.Planning;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static void ResolveInFromBindings(ClauseExecution exec, ParameterBinding[] slotBindings, BlittableJsonReaderObject queryParameters, ValueWriter writer,
        Span<ParameterBinding> bindings, QueryBuilderParameters builderParameters)
    {
        var resolvedValues = new List<object>(bindings.Length);
        var termTypes = new List<ParamValueType>(bindings.Length);
        bool hasNullTerm = false;

        foreach (var templateBinding in bindings)
        {
            // Resolve the per-query slot binding for this leaf; the template binding only carries structure, so
            // the array-valued-parameter pre-check below must inspect the slot binding's name/source too.
            var it = slotBindings[templateBinding.ValueOrdinal];
            if (it.Source == BindingSource.QueryParameter // handle array-valued query parameters
                && queryParameters != null
                && queryParameters.TryGet(it.ParameterName, out object raw)
                && raw is BlittableJsonReaderArray arr)
            {
                foreach (var elem in arr)
                {
                    var (elemVal, elemType) = ResolveParameterValue(elem);
                    AddInValue(elemVal, ToParamValueType(elemType));
                }

                continue;
            }

            var (val, type) = ResolveBindingScalar(it, slotBindings, queryParameters, builderParameters); // normal parameter
            AddInValue(val, type);
        }

        ParamValueType dominantType = resolvedValues.Count > 0 ? termTypes[0] : ParamValueType.String;
        EmitInTerms(exec, writer, dominantType, resolvedValues, hasNullTerm);

        void AddInValue(object val, ParamValueType type)
        {
            if (val == null)
            {
                hasNullTerm = true;
                return;
            }

            resolvedValues.Add(val);
            termTypes.Add(type);
        }
    }
}
