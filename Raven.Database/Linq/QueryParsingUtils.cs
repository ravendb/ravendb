//-----------------------------------------------------------------------
// <copyright file="QueryParsingUtils.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.CodeDom.Compiler;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using ICSharpCode.NRefactory;
using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.PrettyPrinter;
using Lucene.Net.Documents;
using Microsoft.CSharp;
using Raven.Abstractions;
using Raven.Abstractions.MEF;
using Raven.Database.Indexing;
using Raven.Database.Linq.Ast;
using Raven.Database.Linq.PrivateExtensions;
using Raven.Database.Plugins;
using Binder = Microsoft.CSharp.RuntimeBinder.Binder;

namespace Raven.Database.Linq
{
	public static class QueryParsingUtils
	{
		public static string GenerateText(TypeDeclaration type, OrderedPartCollection<AbstractDynamicCompilationExtension> extensions)
		{
			var unit = new CompilationUnit();

			var namespaces = new HashSet<string>
			{
				typeof (SystemTime).Namespace,
				typeof (AbstractViewGenerator).Namespace,
				typeof (Enumerable).Namespace,
				typeof (IEnumerable<>).Namespace,
				typeof (IEnumerable).Namespace,
				typeof (int).Namespace,
				typeof (LinqOnDynamic).Namespace,
				typeof(Field).Namespace,
				typeof(CultureInfo).Namespace,
			};

			foreach (var extension in extensions)
			{
				foreach (var ns in extension.Value.GetNamespacesToImport())
				{
					namespaces.Add(ns);
				}
			}

			foreach (var ns in namespaces)
			{
				unit.AddChild(new Using(ns));
			}

			unit.AddChild(type);
			var output = new CSharpOutputVisitor();
			unit.AcceptVisitor(output, null);

			return output.Text;
		}

		public static string ToText(INode node)
		{
			var output = new CSharpOutputVisitor();
			node.AcceptVisitor(output, null);

			return output.Text;
		}

		public static VariableDeclaration GetVariableDeclarationForLinqQuery(string query, bool requiresSelectNewAnonymousType)
		{
			try
			{
				var parser = ParserFactory.CreateParser(SupportedLanguage.CSharp, new StringReader("var q = " + query));

				var block = parser.ParseBlock();

				if (block.Children.Count != 1)
					throw new InvalidOperationException("Could not understand query: \r\n" + parser.Errors.ErrorOutput);

				var declaration = block.Children[0] as LocalVariableDeclaration;
				if (declaration == null)
					throw new InvalidOperationException("Only local variable declaration are allowed");

				if (declaration.Variables.Count != 1)
					throw new InvalidOperationException("Only one variable declaration is allowed");

				var variable = declaration.Variables[0];

				if (variable.Initializer == null)
					throw new InvalidOperationException("Variable declaration must have an initializer");

				var queryExpression = (variable.Initializer as QueryExpression);
				if (queryExpression == null)
					throw new InvalidOperationException("Variable initializer must be a query expression");

				var selectClause = queryExpression.SelectOrGroupClause as QueryExpressionSelectClause;
				if (selectClause == null)
					throw new InvalidOperationException("Variable initializer must be a select query expression");

				var createExpression = selectClause.Projection as ObjectCreateExpression;
				if ((createExpression == null || createExpression.IsAnonymousType == false) && requiresSelectNewAnonymousType)
					throw new InvalidOperationException(
						"Variable initializer must be a select query expression returning an anonymous object");

				variable.AcceptVisitor(new TransformNullCoalasingOperatorTransformer(), null);
				variable.AcceptVisitor(new DynamicExtensionMethodsTranslator(), null);
				variable.AcceptVisitor(new TransformDynamicLambdaExpressions(), null);
				variable.AcceptVisitor(new TransformObsoleteMethods(), null);
				return variable;
			}
			catch (Exception e)
			{
				throw new InvalidOperationException("Could not understand query: " + Environment.NewLine + query, e);
			}
		}

		public static VariableDeclaration GetVariableDeclarationForLinqMethods(string query, bool requiresSelectNewAnonymousType)
		{
			try
			{
				var parser = ParserFactory.CreateParser(SupportedLanguage.CSharp, new StringReader("var q = " + query));

				var block = parser.ParseBlock();

				if (block.Children.Count != 1)
					throw new InvalidOperationException("Could not understand query: \r\n" + parser.Errors.ErrorOutput);

				var declaration = block.Children[0] as LocalVariableDeclaration;
				if (declaration == null)
					throw new InvalidOperationException("Only local variable declaration are allowed");

				if (declaration.Variables.Count != 1)
					throw new InvalidOperationException("Only one variable declaration is allowed");

				var variable = declaration.Variables[0];

				if (variable.Initializer as InvocationExpression == null)
					throw new InvalidOperationException("Variable declaration must have an initializer which is a method invocation expression");

				var targetObject = ((InvocationExpression)variable.Initializer).TargetObject as MemberReferenceExpression;
				if (targetObject == null)
					throw new InvalidOperationException("Variable initializer must be invoked on a method reference expression");

				if (targetObject.MemberName != "Select" && targetObject.MemberName != "SelectMany")
					throw new InvalidOperationException("Variable initializer must end with a select call");

				var lambdaExpression = AsLambdaExpression(((InvocationExpression)variable.Initializer).Arguments.Last());
				if (lambdaExpression == null)
					throw new InvalidOperationException("Variable initializer select must have a lambda expression");

				variable.AcceptVisitor(new TransformNullCoalasingOperatorTransformer(), null);
				variable.AcceptVisitor(new DynamicExtensionMethodsTranslator(), null);
				variable.AcceptVisitor(new TransformDynamicLambdaExpressions(), null);
				variable.AcceptVisitor(new TransformObsoleteMethods(), null);

				var expressionBody = GetAnonymousCreateExpression(lambdaExpression.ExpressionBody);

				var objectCreateExpression = expressionBody as ObjectCreateExpression;
				if (objectCreateExpression == null && requiresSelectNewAnonymousType)
					throw new InvalidOperationException("Variable initializer select must have a lambda expression with an object create expression");

				if (objectCreateExpression != null && objectCreateExpression.IsAnonymousType == false && objectCreateExpression.CreateType.Type.Contains("Anonymous") == false && requiresSelectNewAnonymousType)
					throw new InvalidOperationException("Variable initializer select must have a lambda expression creating an anonymous type but returning " + objectCreateExpression.CreateType.Type);

				return variable;
			}
			catch (Exception e)
			{
				throw new InvalidOperationException("Could not understand query: " + Environment.NewLine + query, e);
			}
		}

		public static Expression GetAnonymousCreateExpression(Expression expression)
		{
			var invocationExpression = expression as InvocationExpression;

			if (invocationExpression == null)
				return expression;
			var memberReferenceExpression = invocationExpression.TargetObject as MemberReferenceExpression;
			if (memberReferenceExpression == null)
				return expression;
			var typeReference = memberReferenceExpression.TargetObject as TypeReferenceExpression;
			if (typeReference == null)
				return expression;

			if (typeReference.TypeReference.Type != "Raven.Database.Linq.PrivateExtensions.DynamicExtensionMethods")
				return expression;

			switch (memberReferenceExpression.MemberName)
			{
				case "Boost":
					return invocationExpression.Arguments[0];
			}
			return expression;
		}

		public static LambdaExpression AsLambdaExpression(this Expression expression)
		{
			var lambdaExpression = expression as LambdaExpression;
			if (lambdaExpression != null)
				return lambdaExpression;

			var castExpression = expression as CastExpression;
			if (castExpression != null)
			{
				return AsLambdaExpression(castExpression.Expression);
			}

			var parenthesizedExpression = expression as ParenthesizedExpression;
			if (parenthesizedExpression != null)
			{
				return AsLambdaExpression(parenthesizedExpression.Expression);
			}
			return null;
		}

		private class CacheEntry
		{
			public int Usages;
			public string Source;
			public Type Type;
		}

		private static readonly ConcurrentDictionary<string, CacheEntry> cacheEntries = new ConcurrentDictionary<string, CacheEntry>();

		public static Type Compile(string source, string name, string queryText, OrderedPartCollection<AbstractDynamicCompilationExtension> extensions, string basePath)
		{
			source = source.Replace("AbstractIndexCreationTask.SpatialGenerate", "SpatialGenerate"); // HACK, should probably be on the client side

			CacheEntry entry;
			if (cacheEntries.TryGetValue(source, out entry))
			{
				Interlocked.Increment(ref entry.Usages);
				return entry.Type;
			}

			var provider = new CSharpCodeProvider(new Dictionary<string, string> { { "CompilerVersion", "v4.0" } });
			var assemblies = new HashSet<string>
			{
				typeof (SystemTime).Assembly.Location,
				typeof (AbstractViewGenerator).Assembly.Location,
				typeof (NameValueCollection).Assembly.Location,
				typeof (Enumerable).Assembly.Location,
				typeof (Binder).Assembly.Location,
				typeof (Field).Assembly.Location
			};
			foreach (var extension in extensions)
			{
				foreach (var assembly in extension.Value.GetAssembliesToReference())
				{
					assemblies.Add(assembly);
				}
			}
			var compilerParameters = new CompilerParameters
			{
				GenerateExecutable = false,
				GenerateInMemory = true, 
				IncludeDebugInformation = false
			};
			if (basePath != null)
				compilerParameters.TempFiles = new TempFileCollection(basePath, false);

			foreach (var assembly in assemblies)
			{
				compilerParameters.ReferencedAssemblies.Add(assembly);
			}
			var compileAssemblyFromFile = provider.CompileAssemblyFromSource(compilerParameters, source);
			var results = compileAssemblyFromFile;

			if (results.Errors.HasErrors)
			{
				var sb = new StringBuilder()
					.AppendLine("Source code:")
					.AppendLine(queryText)
					.AppendLine();
				foreach (CompilerError error in results.Errors)
				{
					sb.AppendLine(error.ToString());
				}
				throw new InvalidOperationException(sb.ToString());
			}

			CodeVerifier.AssertNoSecurityCriticalCalls(results.CompiledAssembly);

			Type result = results.CompiledAssembly.GetType(name);

			cacheEntries.TryAdd(source, new CacheEntry
			{
				Source = source,
				Type = result,
				Usages = 1
			});

			if (cacheEntries.Count > 256)
			{
				var kvp = cacheEntries.OrderBy(x => x.Value.Usages).FirstOrDefault();
				if (kvp.Key != null)
				{
					CacheEntry _;
					cacheEntries.TryRemove(kvp.Key, out _);
				}
			}

			return result;
		}
	}
}
