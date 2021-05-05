//-----------------------------------------------------------------------
// <copyright file="InMemoryDocumentSessionOperations.Patch.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq.Expressions;
using Lambda2Js;
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
        private readonly JavascriptCompilationOptions _javascriptCompilationOptions;

        public void Increment<T, U>(T entity, Expression<Func<T, U>> path, U valToAdd)
        {
            var metadata = GetMetadataFor(entity);
            var id = metadata.GetString(Constants.Documents.Metadata.Id);
            Increment(id, path, valToAdd);
        }

        public void Increment<T, U>(string id, Expression<Func<T, U>> path, U valToAdd)
        {
            var pathScript = path.CompileToJavascript(_javascriptCompilationOptions);

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

        public void AddOrIncrement<T, TU>(string id, T entity, Expression<Func<T, TU>> patch, TU valToAdd)
        {
            
            var pathScript = patch.CompileToJavascript(_javascriptCompilationOptions);
            
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

        public void AddOrPatch<T, TU>(string id, T entity, Expression<Func<T, List<TU>>> patch, Expression<Func<JavaScriptArray<TU>, object>> arrayAdder)
        {
            var extension = new JavascriptConversionExtensions.CustomMethods {Suffix = _customCount++};
            var pathScript = patch.CompileToJavascript(_javascriptCompilationOptions);
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
                        [Constants.Documents.Metadata.Collection] = collectionName, [Constants.Documents.Metadata.RavenClrType] = clrType
                    }
                });
            
            _valsCount++;

            Defer(new PatchCommandData(id, null, patchRequest) {CreateIfMissing = newInstance});
        }

        public void AddOrPatch<T, TU>(string id, T entity, Expression<Func<T, TU>> patch, TU value)
        {
            var patchScript = patch.CompileToJavascript(_javascriptCompilationOptions);
            var valueToUse = AddTypeNameToValueIfNeeded(patch.Body.Type, value);
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
                        [Constants.Documents.Metadata.Collection] = collectionName, [Constants.Documents.Metadata.RavenClrType] = clrType
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
            var pathScript = path.CompileToJavascript(_javascriptCompilationOptions);

            var valueToUse = AddTypeNameToValueIfNeeded(path.Body.Type, value);

            var patchRequest = new PatchRequest {Script = $"this.{pathScript} = args.val_{_valsCount};", Values = {[$"val_{_valsCount}"] = valueToUse}};

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
            var extension = new JavascriptConversionExtensions.CustomMethods {Suffix = _customCount++};
            var pathScript = path.CompileToJavascript(_javascriptCompilationOptions);
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
            var pathScript = path.CompileToJavascript(_javascriptCompilationOptions);

            if (!(dictionaryAdder.Body is MethodCallExpression call))
            {
                ThrowUnsupportedExpression(dictionaryAdder);
                return; // never hit
            }

            var patchRequest = new PatchRequest();
            object key;
            switch (call.Method.Name)
            {
                case nameof(JavaScriptDictionary<TKey, TValue>.Add):
                    object value;
                    (key, value) = GetKeyAndValue<TKey, TValue>(call);
                    patchRequest.Script = $"this.{pathScript}.{key} = args.val_{_valsCount};";
                    patchRequest.Values[$"val_{_valsCount}"] = value;
                    _valsCount++;
                    break;
                case nameof(JavaScriptDictionary<TKey, TValue>.Remove):
                    key = GetKey(call);
                    patchRequest.Script = $"delete this.{pathScript}.{key};";
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

            return new PatchRequest {Script = script, Values = extension.Parameters};
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

            Defer(new PatchCommandData(id, null, new PatchRequest {Script = newScript, Values = newVals}, null));

            return true;
        }

        private static readonly CreateSerializerOptions SerializerOptions = new CreateSerializerOptions {TypeNameHandling = TypeNameHandling.Objects};

        private object AddTypeNameToValueIfNeeded(Type propertyType, object value)
        {
            if (value == null)
                return null;

            var typeOfValue = value.GetType();
            if (propertyType == typeOfValue || typeOfValue.IsClass == false)
                return value;

            using (var writer = Conventions.Serialization.CreateWriter(Context))
            {
                // the type of the object that's being serialized
                // is not the same as its declared type.
                // so we need to include $type in json

                var serializer = Conventions.Serialization.CreateSerializer(SerializerOptions);

                writer.WriteStartObject();
                writer.WritePropertyName("Value");

                serializer.Serialize(writer, value);

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
    }
}
