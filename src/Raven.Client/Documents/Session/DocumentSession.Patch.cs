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
        private object _patchInfo;

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
            var script = $"this.{pathScript} += val;";

            _documentStore.Operations.Send(new PatchOperation(key, null, patch: new PatchRequest
            {
                Script = script,
                Values = { ["val"] = valToAdd }
            }));

            _patchInfo = new { DocId = key, Script = $"\"{script}\"", Path = path, Val = valToAdd };
        }

        public void Patch<T, U>(string key, Expression<Func<T, U>> path, U value)
        {
            var pathScript = path.CompileToJavascript();
            var script = $"this.{pathScript} = val;";
            _documentStore.Operations.Send(new PatchOperation(key, null, new PatchRequest
            {
                Script = script,
                Values = { ["val"] = value }
            }));

            _patchInfo = new { DocId = key, Script = $"\"{script}\"", Path = path, Val = value };
        }

        public void Patch<T, U>(T entity, Expression<Func<T, U>> path, U value)
        {
            var metadata = GetMetadataFor(entity);
            StringValues id;
            metadata.TryGetValue(Raven.Client.Constants.Documents.Metadata.Id, out id);

            Patch(id, path, value);
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
            var pathScript = path.CompileToJavascript();
            var adderScript = arrayAdder.CompileToJavascript();

            var jsMethodNameAtt = ((MethodCallExpression)arrayAdder.Body).Method.GetCustomAttributes(typeof(JavascriptMethodNameAttribute));
            var name = ((JavascriptMethodNameAttribute)jsMethodNameAtt.ElementAt(0)).Name;

            var script = name == "concat" ? $"this.{pathScript} = this.{pathScript}{adderScript}" : $"this.{pathScript}{adderScript}";

            object val;
            var arg = ((MethodCallExpression)arrayAdder.Body).Arguments[0];
            LinqPathProvider.GetValueFromExpressionWithoutConversion(arg, out val);

            _documentStore.Operations.Send(new PatchOperation(key, null, new PatchRequest
            {
                Script = script,
                Values = { { "val", val } }
            }));

            _patchInfo = new { DocId = key, Script = $"\"{script}\"", Path = path, Val = val };
        }

        public override string ToString()
        {
            return _patchInfo.ToString();
        }
    }
}