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
using System.Text;
using Lambda2Js;
using Microsoft.Extensions.Primitives;
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
            public string methodName;
            public Dictionary<string, object> Parameters = new Dictionary<string, object>();
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

                methodName = nameAttribute.Name;
                var javascriptWriter = context.GetWriter();
                javascriptWriter.Write(".");
                javascriptWriter.Write(methodName);
                javascriptWriter.Write("(");

                for (int i = 0; i < methodCallExpression.Arguments.Count; i++)
                {
                    var name = "arg_" + Parameters.Count;
                    if (i != 0)
                        javascriptWriter.Write(", ");
                    javascriptWriter.Write(name);
                    Parameters[name] = methodCallExpression.Arguments[i];
                }
                if (nameAttribute.PositionalArguments != null)
                {
                    for (int i = methodCallExpression.Arguments.Count;
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

        public void Increment<T, U>(T entity, Expression<Func<T, U>> path, U valToAdd)
        {
            var metadata = GetMetadataFor(entity);
            StringValues id;
            metadata.TryGetValue(Constants.Documents.Metadata.Id, out id);

            Increment(id, path, valToAdd);
        }

        public void Increment<T, U>(string key, Expression<Func<T, U>> path, U valToAdd)
        {
            var pathScript = path.CompileToJavascript();

            Advanced.Defer();

            _documentStore.Operations.Send(new PatchOperation(key, null, new PatchRequest
            {
                Script = $"this.{pathScript} += val;",
                Values = { ["val"] = valToAdd }
            }));
        }

        public void Patch<T, U>(T entity, Expression<Func<T, U>> path, U value)
        {
            var metadata = GetMetadataFor(entity);
            StringValues id;
            metadata.TryGetValue(Constants.Documents.Metadata.Id, out id);

            Patch(id, path, value);
        }

        public void Patch<T, U>(string key, Expression<Func<T, U>> path, U value)
        {
            var pathScript = path.CompileToJavascript();

            Advanced.Defer();

            _documentStore.Operations.Send(new PatchOperation(key, null, new PatchRequest
            {
                Script = $"this.{pathScript} = val;",
                Values = { ["val"] = value }
            }));
        }

        public void Patch<T, U>(T entity, Expression<Func<T, IEnumerable<U>>> path,
            Expression<Func<JavaScriptArray<U>, object>> arrayAdder)
        {
            var metadata = GetMetadataFor(entity);
            StringValues id;
            metadata.TryGetValue(Constants.Documents.Metadata.Id, out id);

            Patch(id, path, arrayAdder);
        }

        public void Patch<T, U>(string key, Expression<Func<T, IEnumerable<U>>> path,
            Expression<Func<JavaScriptArray<U>, object>> arrayAdder)
        {
            var extension = new CustomMethods();
            var pathScript = path.CompileToJavascript();
            var adderScript = arrayAdder.CompileToJavascript(
                new JavascriptCompilationOptions(
                    JsCompilationFlags.BodyOnly | JsCompilationFlags.ScopeParameter,
                    new LinqMethods(), extension));

            var script = extension.methodName == "concat" ? $"this.{pathScript} = this.{pathScript}{adderScript}" : $"this.{pathScript}{adderScript}";

            object val;
            var arg = ((MethodCallExpression)arrayAdder.Body).Arguments[0];
            LinqPathProvider.GetValueFromExpressionWithoutConversion(arg, out val);

            Advanced.Defer();

            _documentStore.Operations.Send(new PatchOperation(key, null, new PatchRequest
            {
                Script = script,
                Values = { { "arg_0", val } }
            }));
        }
    }
}