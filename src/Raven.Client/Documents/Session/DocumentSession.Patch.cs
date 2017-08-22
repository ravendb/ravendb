//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Lambda2Js;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        private class CustomMethods : JavascriptConversionExtension
        {
            public readonly Dictionary<string, object> Parameters = new Dictionary<string, object>();
            public int Suffix { get; set;}

            public override void ConvertToJavascript(JavascriptConversionContext context)
            {
                var methodCallExpression = context.Node as MethodCallExpression;

                var nameAttribute = methodCallExpression?
                    .Method
                    .GetCustomAttributes(typeof(JavascriptMethodNameAttribute), false)
                    .OfType<JavascriptMethodNameAttribute>()
                    .FirstOrDefault();

                if (nameAttribute == null)
                    return;
                context.PreventDefault();

                var javascriptWriter = context.GetWriter();
                javascriptWriter.Write(".");
                javascriptWriter.Write(nameAttribute.Name);
                javascriptWriter.Write("(");

                var args = new List<Expression>();
                foreach (var expr in methodCallExpression.Arguments)
                {
                    var expression = expr as NewArrayExpression;
                    if (expression != null)
                        args.AddRange(expression.Expressions);
                    else
                        args.Add(expr);
                }

                for (var i = 0; i < args.Count; i++)
                {
                    var name = $"arg_{Parameters.Count}_{Suffix}";
                    if (i != 0)
                        javascriptWriter.Write(", ");
                    javascriptWriter.Write("args.");
                    javascriptWriter.Write(name);
                    object val;
                    if (LinqPathProvider.GetValueFromExpressionWithoutConversion(args[i], out val))
                        Parameters[name] = val;
                }
                if (nameAttribute.PositionalArguments != null)
                {
                    for (int i = args.Count;
                        i < nameAttribute.PositionalArguments.Length;
                        i++)
                    {
                        if (i != 0)
                            javascriptWriter.Write(", ");
                        context.Visitor.Visit(Expression.Constant(nameAttribute.PositionalArguments[i]));
                    }
                }

                javascriptWriter.Write(")");
            }
        }

        private int _valsCount;
        private int _customCount;

        public void Increment<T, U>(T entity, Expression<Func<T, U>> path, U valToAdd)
        {
            var metadata = GetMetadataFor(entity);
            var id = metadata.GetString(Constants.Documents.Metadata.Id);
            Increment(id, path, valToAdd);
        }

        public void Increment<T, U>(string id, Expression<Func<T, U>> path, U valToAdd)
        {
            var pathScript = path.CompileToJavascript();

            var patchRequest = new PatchRequest
            {
                Script = $"this.{pathScript} += args.val_{_valsCount};",
                Values = {[$"val_{_valsCount}"] = valToAdd} 
            };

            _valsCount++;

            if (TryMergePatches(id, patchRequest) == false)
            {
                Advanced.Defer(new PatchCommandData(id, null, patchRequest, null));
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
            var pathScript = path.CompileToJavascript();

            var patchRequest = new PatchRequest
            {
                Script = $"this.{pathScript} = args.val_{_valsCount};",
                Values = {[$"val_{_valsCount}"] = value}
            };

            _valsCount++;

            if (TryMergePatches(id, patchRequest) == false)
            {
                Advanced.Defer(new PatchCommandData(id, null, patchRequest, null));
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
            var extension = new CustomMethods
            {
                Suffix = _customCount++
            };
            var pathScript = path.CompileToJavascript();
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
                Advanced.Defer(new PatchCommandData(id, null, patchRequest, null));
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

            Advanced.Defer(new PatchCommandData(id, null, new PatchRequest
            {
                Script = newScript,
                Values = newVals
            }, null));

            return true;
        }
    }
}
