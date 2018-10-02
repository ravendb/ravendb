//-----------------------------------------------------------------------
// <copyright file="InMemoryDocumentSessionOperations.Patch.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Lambda2Js;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
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
                Values = { [$"val_{_valsCount}"] = valToAdd }
            };

            _valsCount++;

            if (TryMergePatches(id, patchRequest) == false)
            {
                Defer(new PatchCommandData(id, null, patchRequest, null));
            }
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

            var patchRequest = new PatchRequest
            {
                Script = $"this.{pathScript} = args.val_{_valsCount};",
                Values = { [$"val_{_valsCount}"] = value }
            };

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
            var extension = new JavascriptConversionExtensions.CustomMethods
            {
                Suffix = _customCount++
            };
            var pathScript = path.CompileToJavascript(_javascriptCompilationOptions);
            var adderScript = arrayAdder.CompileToJavascript(
                new JavascriptCompilationOptions(
                    JsCompilationFlags.BodyOnly | JsCompilationFlags.ScopeParameter,
                    new LinqMethods(), extension));

            var patchRequest = new PatchRequest
            {
                Script = $"this.{pathScript}{adderScript}",
                Values = extension.Parameters
            };

            if (TryMergePatches(id, patchRequest) == false)
            {
                Defer(new PatchCommandData(id, null, patchRequest, null));
            }
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

            Defer(new PatchCommandData(id, null, new PatchRequest
            {
                Script = newScript,
                Values = newVals
            }, null));

            return true;
        }
    }
}
