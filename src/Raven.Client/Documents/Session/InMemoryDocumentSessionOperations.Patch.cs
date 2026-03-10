using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq.Expressions;
using Lambda2Js;
using Microsoft.AspNetCore.JsonPatch;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Abstract implementation for in memory session operations
    /// </summary>
    public abstract partial class InMemoryDocumentSessionOperations
    {
        private int _valsCount;
        private int _customCount;
        private readonly Lazy<JavascriptCompilationOptions> _pathScriptCompilationOptions;

        public void Increment<T, U>(T entity, Expression<Func<T, U>> path, U valToAdd)
        {
            var metadata = GetMetadataFor(entity);
            var id = metadata.GetString(Constants.Documents.Metadata.Id);
            Increment(id, path, valToAdd);
        }

        public void Increment<T, U>(string id, Expression<Func<T, U>> path, U valToAdd)
        {
            var pathScript = path.CompileToJavascript(_pathScriptCompilationOptions.Value);

            var variable = $"this.{pathScript}";
            var value = $"args.val_{_valsCount}";

            var patchRequest = new PatchRequest
            {
                Script = $"{variable} = {variable} ? {variable} + {value} : {value};",
                Values =
                {
                    [$"val_{_valsCount}"] = valToAdd
                }
            };

            _valsCount++;

            if (TryMergePatches(id, patchRequest) == false)
            {
                Defer(new PatchCommandData(id,
                    null,
                    patchRequest,
                    null));
            }
        }

        public void AddOrIncrement<T, TU>(string id, T entity, Expression<Func<T, TU>> path, TU valueToAdd)
        {

            var pathScript = path.CompileToJavascript(_pathScriptCompilationOptions.Value);

            var variable = $"this.{pathScript}";
            var value = $"args.val_{_valsCount}";

            var patchRequest = new PatchRequest
            {
                Script = $"{variable} = {variable} ? {variable} + {value} : {value};",
                Values =
                {
                    [$"val_{_valsCount}"] = valueToAdd
                }
            };

            string collectionName = _requestExecutor.Conventions.GetCollectionName(entity);
            string clrType = _requestExecutor.Conventions.GetClrTypeName(entity);
            var newInstance = JsonConverter.ToBlittable(
                entity,
                new DocumentInfo
                {
                    Id = id,
                    Collection = collectionName,
                    MetadataInstance = new MetadataAsDictionary
                    {
                        [Constants.Documents.Metadata.Collection] = collectionName,
                        [Constants.Documents.Metadata.RavenClrType] = clrType
                    }
                });

            _valsCount++;

            Defer(new PatchCommandData(id,
                null,
                patchRequest)
            {
                CreateIfMissing = newInstance
            });
        }

        public void AddOrPatch<T, TU>(string id, T entity, Expression<Func<T, List<TU>>> path, Expression<Func<JavaScriptArray<TU>, object>> arrayAdder)
        {
            var extension = new JavascriptConversionExtensions.CustomMethods
            {
                Suffix = _customCount++,
                SaveEnumsAsIntegersForPatching = DocumentStore.Conventions.SaveEnumsAsIntegersForPatching,
            };
            var pathScript = path.CompileToJavascript(_pathScriptCompilationOptions.Value);
            var adderScript = arrayAdder.CompileToJavascript(
                new JavascriptCompilationOptions(
                    JsCompilationFlags.BodyOnly | JsCompilationFlags.ScopeParameter,
                    new LinqMethods(),
                    extension,
                    JavascriptConversionExtensions.ToStringSupport.Instance,
                    JavascriptConversionExtensions.ConstantSupport.Instance)
            );

            var patchRequest = CreatePatchRequest(arrayAdder, pathScript, adderScript, extension);
            string collectionName = _requestExecutor.Conventions.GetCollectionName(entity);
            string clrType = _requestExecutor.Conventions.GetClrTypeName(entity);
            var newInstance = JsonConverter.ToBlittable(entity,
                new DocumentInfo
                {
                    Id = id,
                    Collection = collectionName,
                    MetadataInstance = new MetadataAsDictionary
                    {
                        [Constants.Documents.Metadata.Collection] = collectionName,
                        [Constants.Documents.Metadata.RavenClrType] = clrType
                    }
                });

            _valsCount++;

            Defer(new PatchCommandData(id, null, patchRequest) { CreateIfMissing = newInstance });
        }

        public void AddOrPatch<T, TU>(string id, T entity, Expression<Func<T, TU>> path, TU value)
        {
            var patchScript = path.CompileToJavascript(_pathScriptCompilationOptions.Value);
            var valueToUse = AddTypeNameToValueIfNeeded(path.Body.Type, value);
            if (DocumentStore.Conventions.SaveEnumsAsIntegersForPatching && value is Enum)
            {
                valueToUse = Convert.ToInt32(value);
            }
            var patchRequest = new PatchRequest
            {
                Script = $"this.{patchScript} = args.val_{_valsCount};",
                Values =
                {
                    [$"val_{_valsCount}"] = valueToUse
                }
            };

            string collectionName = _requestExecutor.Conventions.GetCollectionName(entity);
            string clrType = _requestExecutor.Conventions.GetClrTypeName(entity);
            var newInstance = JsonConverter.ToBlittable(entity,
                new DocumentInfo
                {
                    Id = id,
                    Collection = collectionName,
                    MetadataInstance = new MetadataAsDictionary
                    {
                        [Constants.Documents.Metadata.Collection] = collectionName,
                        [Constants.Documents.Metadata.RavenClrType] = clrType
                    }
                });

            _valsCount++;

            Defer(new PatchCommandData(id,
                null,
                patchRequest)
            {
                CreateIfMissing = newInstance
            });
        }

        public void Patch<T, U>(T entity, Expression<Func<T, U>> path, U value)
        {
            var metadata = GetMetadataFor(entity);
            var id = metadata.GetString(Constants.Documents.Metadata.Id);
            Patch(id, path, value);
        }

        public void Patch<T, U>(string id, Expression<Func<T, U>> path, U value)
        {
            if (ShouldUseJsonPatch(path.Body.Type, value) && HasExistingJavaScriptPatch(id) == false
                && TryBuildJsonPointer(path.Body, out var jsonPointer))
            {
                var jpd = new JsonPatchDocument();
                jpd.Replace(jsonPointer, ConvertValueForJsonPatch(value));

                if (TryMergeJsonPatches(id, jpd) == false)
                    Defer(new JsonPatchCommandData(id, jpd));

                return;
            }

            var pathScript = path.CompileToJavascript(_pathScriptCompilationOptions.Value);
            var valueForJs = AddTypeNameToValueIfNeeded(path.Body.Type, value);
            if (DocumentStore.Conventions.SaveEnumsAsIntegersForPatching && value is Enum)
            {
                valueForJs = Convert.ToInt32(value);
            }

            var patchRequest = new PatchRequest { Script = $"this.{pathScript} = args.val_{_valsCount};", Values = { [$"val_{_valsCount}"] = valueForJs } };

            _valsCount++;

            if (TryMergePatches(id, patchRequest) == false)
            {
                Defer(new PatchCommandData(id, null, patchRequest, null));
            }
        }

        public void Patch<T, U>(T entity, Expression<Func<T, IEnumerable<U>>> path,
            Expression<Func<JavaScriptArray<U>, object>> arrayAdder)
        {
            var metadata = GetMetadataFor(entity);
            var id = metadata.GetString(Constants.Documents.Metadata.Id);
            Patch(id, path, arrayAdder);
        }

        public void Patch<T, U>(string id, Expression<Func<T, IEnumerable<U>>> path,
            Expression<Func<JavaScriptArray<U>, object>> arrayAdder)
        {
            if (HasExistingJavaScriptPatch(id) == false && TryCreateArrayJsonPatch(id, path, arrayAdder))
                return;

            var extension = new JavascriptConversionExtensions.CustomMethods
            {
                Suffix = _customCount++,
                SaveEnumsAsIntegersForPatching = DocumentStore.Conventions.SaveEnumsAsIntegersForPatching,
            };
            var pathScript = path.CompileToJavascript(_pathScriptCompilationOptions.Value);
            var adderScript = arrayAdder.CompileToJavascript(
                new JavascriptCompilationOptions(
                    JsCompilationFlags.BodyOnly | JsCompilationFlags.ScopeParameter,
                    new LinqMethods(),
                    extension,
                    JavascriptConversionExtensions.ToStringSupport.Instance,
                    JavascriptConversionExtensions.ConstantSupport.Instance));

            var patchRequest = CreatePatchRequest(arrayAdder, pathScript, adderScript, extension);

            if (TryMergePatches(id, patchRequest) == false)
            {
                Defer(new PatchCommandData(id, null, patchRequest, null));
            }
        }

        public void Patch<T, TKey, TValue>(string id, Expression<Func<T, IDictionary<TKey, TValue>>> path,
            Expression<Func<JavaScriptDictionary<TKey, TValue>, object>> dictionaryAdder)
        {
            if (!(dictionaryAdder.Body is MethodCallExpression call))
            {
                ThrowUnsupportedExpression(dictionaryAdder);
                return; // never hit
            }

            if (HasExistingJavaScriptPatch(id) == false && TryBuildJsonPointer(path.Body, out var jsonPointer))
            {
                switch (call.Method.Name)
                {
                    case nameof(JavaScriptDictionary<TKey, TValue>.Add):
                    {
                        var (dictKey, dictValue) = GetKeyAndValue<TKey, TValue>(call);
                        if (ShouldUseJsonPatch(typeof(TValue), dictValue))
                        {
                            var escapedKey = EscapeJsonPointerSegment(dictKey.ToString());
                            var jpd = new JsonPatchDocument();

                            jpd.Add($"{jsonPointer}/{escapedKey}", ConvertValueForJsonPatch(dictValue));

                            if (TryMergeJsonPatches(id, jpd) == false)
                                Defer(new JsonPatchCommandData(id, jpd));

                            return;
                        }

                        break;
                    }
                    case nameof(JavaScriptDictionary<TKey, TValue>.Remove):
                    {
                        var dictKey = GetKey(call);
                        var escapedKey = EscapeJsonPointerSegment(dictKey.ToString());
                        var jpd = new JsonPatchDocument();
                        jpd.Remove($"{jsonPointer}/{escapedKey}");

                        if (TryMergeJsonPatches(id, jpd) == false)
                            Defer(new JsonPatchCommandData(id, jpd));

                        return;
                    }
                }
            }

            var pathScript = path.CompileToJavascript(_pathScriptCompilationOptions.Value);
            var patchRequest = new PatchRequest();
            object key;
            switch (call.Method.Name)
            {
                case nameof(JavaScriptDictionary<TKey, TValue>.Add):
                    object value;
                    (key, value) = GetKeyAndValue<TKey, TValue>(call);
                    var formattedKey = FormatKeyForJavaScript(key);
                    patchRequest.Script = $"this.{pathScript}[{formattedKey}] = args.val_{_valsCount};";
                    if (DocumentStore.Conventions.SaveEnumsAsIntegersForPatching && value is Enum)
                    {
                        value = Convert.ToInt32(value);
                    }
                    patchRequest.Values[$"val_{_valsCount}"] = value;
                    _valsCount++;
                    break;
                case nameof(JavaScriptDictionary<TKey, TValue>.Remove):
                    key = GetKey(call);
                    formattedKey = FormatKeyForJavaScript(key);
                    patchRequest.Script = $"delete this.{pathScript}[{formattedKey}];";
                    break;
                default:
                    throw new InvalidOperationException("Unsupported method: " + call.Method.Name);
            }

            if (TryMergePatches(id, patchRequest) == false)
            {
                Defer(new PatchCommandData(id, null, patchRequest, null));
            }
        }

        public void Patch<T, TKey, TValue>(T entity, Expression<Func<T, IDictionary<TKey, TValue>>> path,
            Expression<Func<JavaScriptDictionary<TKey, TValue>, object>> dictionaryAdder)
        {
            var metadata = GetMetadataFor(entity);
            var id = metadata.GetString(Constants.Documents.Metadata.Id);
            Patch(id, path, dictionaryAdder);
        }

        private static PatchRequest CreatePatchRequest<T>(Expression<Func<JavaScriptArray<T>, object>> arrayAdder, string pathScript, string adderScript,
            JavascriptConversionExtensions.CustomMethods extension)
        {
            var script = $"this.{pathScript}{adderScript}";

            if (arrayAdder.Body is MethodCallExpression mce &&
                mce.Method.Name == nameof(JavaScriptArray<T>.RemoveAll))
            {
                script = $"this.{pathScript} = {script}";
            }

            return new PatchRequest { Script = script, Values = extension.Parameters };
        }

        private bool TryMergePatches(string id, PatchRequest patchRequest)
        {
            if (DeferredCommandsDictionary.TryGetValue((id, CommandType.PATCH, null), out ICommandData command) == false)
                return false;

            DeferredCommands.Remove(command);
            // We'll overwrite the DeferredCommandsDictionary when calling Defer
            // No need to call DeferredCommandsDictionary.Remove((id, CommandType.PATCH, null));

            var oldPatch = (PatchCommandData)command;
            var newScript = oldPatch.Patch.Script + '\n' + patchRequest.Script;
            var newVals = oldPatch.Patch.Values;

            foreach (var kvp in patchRequest.Values)
            {
                newVals[kvp.Key] = kvp.Value;
            }

            Defer(new PatchCommandData(id, null, new PatchRequest { Script = newScript, Values = newVals }, null));

            return true;
        }

        private static readonly CreateSerializerOptions SerializerOptions = new CreateSerializerOptions { TypeNameHandling = TypeNameHandling.Auto };

        private object AddTypeNameToValueIfNeeded(Type propertyType, object value)
        {
            if (value == null)
                return null;

            var typeOfValue = value.GetType();
            if (
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
                value is not (DateOnly or TimeOnly)
                &&
#endif
                (propertyType == typeOfValue || typeOfValue.IsClass == false))
                return value;

            using (var writer = Conventions.Serialization.CreateWriter(Context))
            {
                // the type of the object that's being serialized
                // is not the same as its declared type.
                // so we need to include $type in json

                var serializer = Conventions.Serialization.CreateSerializer(SerializerOptions);

                writer.WriteStartObject();
                writer.WritePropertyName("Value");

                serializer.Serialize(writer, value, propertyType);

                writer.WriteEndObject();

                writer.FinalizeDocument();

                var reader = writer.CreateReader();

                return reader["Value"];
            }
        }

        private static (object Key, object Value) GetKeyAndValue<TKey, TValue>(MethodCallExpression call)
        {
            if (call.Arguments.Count == 1)
            {
                if (LinqPathProvider.GetValueFromExpressionWithoutConversion(call.Arguments[0], out object obj) == false)
                    ThrowUnsupportedExpression(call.Arguments[0]);
                if (!(obj is KeyValuePair<TKey, TValue> kvp))
                    throw new InvalidOperationException("Unexpected argument type: " + obj.GetType());
                return (kvp.Key, kvp.Value);
            }

            Debug.Assert(call.Arguments.Count == 2);

            object key, value;
            if (call.Arguments[0] is ConstantExpression c)
                key = c.Value;
            else if (LinqPathProvider.GetValueFromExpressionWithoutConversion(call.Arguments[0], out key) == false)
                ThrowUnsupportedExpression(call.Arguments[0]);

            if (call.Arguments[1] is ConstantExpression c2)
                value = c2.Value;
            else if (LinqPathProvider.GetValueFromExpressionWithoutConversion(call.Arguments[1], out value) == false)
                ThrowUnsupportedExpression(call.Arguments[1]);

            return (key, value);
        }

        private static object GetKey(MethodCallExpression call)
        {
            if (call.Arguments[0] is ConstantExpression c)
                return c.Value;

            if (LinqPathProvider.GetValueFromExpressionWithoutConversion(call.Arguments[0], out object obj) == false)
                ThrowUnsupportedExpression(call.Arguments[0]);

            return obj;
        }

        private static void ThrowUnsupportedExpression(Expression expression)
        {
            throw new InvalidOperationException("Unsupported expression: " + expression);
        }

        private static string FormatKeyForJavaScript(object key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key), "Dictionary key cannot be null");

            return JavascriptConversionExtensions.ToJsStringLiteral(key.ToString());
        }

        private static string EscapeJsonPointerSegment(string segment)
        {
            return segment.Replace("~", "~0").Replace("/", "~1");
        }

        private bool TryBuildJsonPointer(Expression body, out string jsonPointer)
        {
            jsonPointer = null;
            var segments = new List<string>();
            var current = body;

            while (current != null)
            {
                switch (current)
                {
                    case ParameterExpression:
                        // reached the root parameter (x =>), done
                        current = null;
                        break;

                    case MemberExpression member:
                        var name = Conventions.GetConvertedPropertyNameFor(member.Member);
                        segments.Add(EscapeJsonPointerSegment(name));
                        current = member.Expression;
                        break;

                    case BinaryExpression { NodeType: ExpressionType.ArrayIndex } arrayIndex:
                        if (arrayIndex.Right is ConstantExpression indexConst && indexConst.Value is int idx)
                        {
                            segments.Add(idx.ToString());
                            current = arrayIndex.Left;
                        }
                        else
                        {
                            return false;
                        }
                        break;

                    case MethodCallExpression call when call.Method.Name == "get_Item" && call.Arguments.Count == 1:
                        if (call.Arguments[0] is ConstantExpression itemConst && itemConst.Value is int itemIdx)
                        {
                            segments.Add(itemIdx.ToString());
                            current = call.Object;
                        }
                        else
                        {
                            return false;
                        }
                        break;

                    case UnaryExpression unary:
                        current = unary.Operand;
                        break;

                    default:
                        return false;
                }
            }

            segments.Reverse();
            jsonPointer = "/" + string.Join("/", segments);
            return true;
        }

        private static bool ShouldUseJsonPatch(Type propertyType, object value)
        {
            if (value == null)
                return true;

            var typeOfValue = value.GetType();

#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
            if (value is DateOnly or TimeOnly)
                return false;
#endif

            if (typeOfValue.IsClass && typeOfValue != typeof(string))
                return false;

            return true;
        }

        private bool TryMergeJsonPatches(string id, JsonPatchDocument patch)
        {
            if (DeferredCommandsDictionary.TryGetValue((id, CommandType.JsonPatch, null), out ICommandData command) == false)
                return false;

            DeferredCommands.Remove(command);

            var oldPatch = (JsonPatchCommandData)command;
            foreach (var op in patch.Operations)
            {
                oldPatch.JsonPatch.Operations.Add(op);
            }

            Defer(oldPatch);
            return true;
        }

        private bool HasExistingJavaScriptPatch(string id)
        {
            return DeferredCommandsDictionary.ContainsKey((id, CommandType.PATCH, null));
        }

        private object ConvertValueForJsonPatch(object value)
        {
            if (value is not Enum)
                return value;

            return DocumentStore.Conventions.SaveEnumsAsIntegersForPatching
                ? Convert.ToInt32(value)
                : value.ToString();
        }

        private bool TryCreateArrayJsonPatch<T, U>(string id, Expression<Func<T, IEnumerable<U>>> path,
            Expression<Func<JavaScriptArray<U>, object>> arrayAdder)
        {
            var operations = new List<(string MethodName, List<object> Values)>();
            if (CollectArrayOperations(arrayAdder.Body, operations) == false)
                return false;

            if (TryBuildJsonPointer(path.Body, out var jsonPointer) == false)
                return false;

            var jpd = new JsonPatchDocument();

            foreach (var (methodName, values) in operations)
            {
                switch (methodName)
                {
                    case nameof(JavaScriptArray<U>.Add):
                        foreach (var val in values)
                        {
                            if (ShouldUseJsonPatch(typeof(U), val) == false)
                                return false;

                            jpd.Add($"{jsonPointer}/-", ConvertValueForJsonPatch(val));
                        }
                        break;
                    case nameof(JavaScriptArray<U>.RemoveAt):
                        jpd.Remove($"{jsonPointer}/{values[0]}");
                        break;
                    default:
                        return false;
                }
            }

            if (TryMergeJsonPatches(id, jpd) == false)
                Defer(new JsonPatchCommandData(id, jpd));

            return true;
        }

        private static bool CollectArrayOperations(Expression expression, List<(string MethodName, List<object> Values)> operations)
        {
            if (expression is UnaryExpression unary)
                expression = unary.Operand;

            if (expression is not MethodCallExpression mce)
                return false;

            // Handle chaining: the Object of the method call may itself be a method call
            if (mce.Object is MethodCallExpression innerCall)
            {
                if (CollectArrayOperations(innerCall, operations) == false)
                    return false;
            }

            var methodName = mce.Method.Name;

            if (methodName == nameof(JavaScriptArray<object>.RemoveAll))
                return false;

            var values = new List<object>();
            foreach (var arg in mce.Arguments)
            {
                if (arg.Type.IsArray)
                {
                    if (arg is NewArrayExpression newArray)
                    {
                        foreach (var element in newArray.Expressions)
                        {
                            if (LinqPathProvider.GetValueFromExpressionWithoutConversion(element, out var val) == false)
                                return false;
                            values.Add(val);
                        }
                    }
                    else if (LinqPathProvider.GetValueFromExpressionWithoutConversion(arg, out var arrayValue) && arrayValue is Array array)
                    {
                        foreach (var item in array)
                            values.Add(item);
                    }
                    else
                    {
                        return false;
                    }
                }
                else
                {
                    if (LinqPathProvider.GetValueFromExpressionWithoutConversion(arg, out var val) == false)
                        return false;
                    values.Add(val);
                }
            }

            operations.Add((methodName, values));
            return true;
        }
    }
}
