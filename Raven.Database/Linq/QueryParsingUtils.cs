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
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using ICSharpCode.NRefactory;
using ICSharpCode.NRefactory.CSharp;
using ICSharpCode.NRefactory.PatternMatching;
using Lucene.Net.Documents;
using Microsoft.CSharp;
using Raven.Abstractions;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util;
using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Linq.Ast;
using Raven.Database.Linq.PrivateExtensions;
using Raven.Database.Plugins;
using Raven.Database.Server;
using Raven.Database.Storage;
using Binder = Microsoft.CSharp.RuntimeBinder.Binder;

namespace Raven.Database.Linq
{
	public static class QueryParsingUtils
	{
		[CLSCompliant(false)]
		public static string GenerateText(TypeDeclaration type, 
			OrderedPartCollection<AbstractDynamicCompilationExtension> extensions,
			HashSet<string> namespaces = null)
		{
			var unit = new SyntaxTree();

			if (namespaces == null)
			{
				namespaces = new HashSet<string>
				{
					typeof (SystemTime).Namespace,
					typeof (AbstractViewGenerator).Namespace,
					typeof (Enumerable).Namespace,
					typeof (IEnumerable<>).Namespace,
					typeof (IEnumerable).Namespace,
					typeof (int).Namespace,
					typeof (LinqOnDynamic).Namespace,
					typeof (Field).Namespace,
					typeof (CultureInfo).Namespace,
					typeof (Regex).Namespace
				};
			}

			foreach (var extension in extensions)
			{
				foreach (var ns in extension.Value.GetNamespacesToImport())
				{
					namespaces.Add(ns);
				}
			}

			foreach (var ns in namespaces)
			{
				unit.Members.Add(new UsingDeclaration(ns));
			}

			unit.Members.Add(new WindowsNewLine());

			unit.Members.Add(type);

			var stringWriter = new StringWriter();
			var output = new CSharpOutputVisitor(stringWriter, FormattingOptionsFactory.CreateSharpDevelop());
			unit.AcceptVisitor(output);

			return stringWriter.GetStringBuilder().ToString();
		}

		[CLSCompliant(false)]
		public static string ToText(AstNode node)
		{
			var stringWriter = new StringWriter();
			var output = new CSharpOutputVisitor(stringWriter, FormattingOptionsFactory.CreateSharpDevelop());
			node.AcceptVisitor(output);

			return stringWriter.GetStringBuilder().ToString();
		}

		[CLSCompliant(false)]
		public static VariableInitializer GetVariableDeclarationForLinqQuery(string query, bool requiresSelectNewAnonymousType)
		{
			try
			{
				var parser = new CSharpParser();
				var block = parser.ParseStatements(ToQueryStatement(query)).ToList();

				if (block.Count == 0 || parser.HasErrors)
				{
					var errs = string.Join(Environment.NewLine, parser.Errors.Select(x => x.Region + ": " + x.ErrorType + " - " + x.Message));
					throw new InvalidOperationException("Could not understand query: \r\n" + errs);
				}

				var declaration = block[0] as VariableDeclarationStatement;
				if (declaration == null)
					throw new InvalidOperationException("Only local variable declaration are allowed");

				if (declaration.Variables.Count != 1)
					throw new InvalidOperationException("Only one variable declaration is allowed");

				var variable = declaration.Variables.First();

				if (variable.Initializer == null)
					throw new InvalidOperationException("Variable declaration must have an initializer");

				var queryExpression = (variable.Initializer as QueryExpression);
				if (queryExpression == null)
					throw new InvalidOperationException("Variable initializer must be a query expression");

				var selectClause = queryExpression.Clauses.OfType<QuerySelectClause>().FirstOrDefault();
				if (selectClause == null)
					throw new InvalidOperationException("Variable initializer must be a select query expression");

				var createExpression = GetAnonymousCreateExpression(selectClause.Expression) as AnonymousTypeCreateExpression;
				if ((createExpression == null) && requiresSelectNewAnonymousType)
					throw new InvalidOperationException(
						"Variable initializer must be a select query expression returning an anonymous object");

				variable.AcceptVisitor(new TransformNullCoalescingOperatorTransformer(), null);
				variable.AcceptVisitor(new DynamicExtensionMethodsTranslator(), null);
				variable.AcceptVisitor(new TransformDynamicLambdaExpressions(), null);
                variable.AcceptVisitor(new TransformDynamicInvocationExpressions(), null);
				variable.AcceptVisitor(new TransformObsoleteMethods(), null);
				return variable;
			}
			catch (Exception e)
			{
				throw new InvalidOperationException("Could not understand query: " + e.Message, e);
			}
		}

		[CLSCompliant(false)]
		public static VariableInitializer GetVariableDeclarationForLinqMethods(string query, bool requiresSelectNewAnonymousType)
		{
			try
			{

				var parser = new CSharpParser();

				var block = parser.ParseStatements(ToQueryStatement(query)).ToList();

				if (block.Count == 0 || parser.HasErrors)
				{
					var errs = string.Join(Environment.NewLine, parser.Errors.Select(x => x.Region + ": " + x.ErrorType + " - " + x.Message));
					throw new InvalidOperationException("Could not understand query: \r\n" + errs);
				}

				var declaration = block[0] as VariableDeclarationStatement;
				if (declaration == null)
					throw new InvalidOperationException("Only local variable declaration are allowed");

				if (declaration.Variables.Count != 1)
					throw new InvalidOperationException("Only one variable declaration is allowed");

				var variable = declaration.Variables.First();

				if (variable.Initializer as InvocationExpression == null)
					throw new InvalidOperationException("Variable declaration must have an initializer which is a method invocation expression");

				var targetObject = ((InvocationExpression)variable.Initializer).Target as MemberReferenceExpression;
				if (targetObject == null)
					throw new InvalidOperationException("Variable initializer must be invoked on a method reference expression");

				if (targetObject.MemberName != "Select" && targetObject.MemberName != "SelectMany")
					throw new InvalidOperationException("Variable initializer must end with a select call");

				var lambdaExpression = AsLambdaExpression(((InvocationExpression)variable.Initializer).Arguments.Last());
				if (lambdaExpression == null)
					throw new InvalidOperationException("Variable initializer select must have a lambda expression");

				variable.AcceptVisitor(new TransformNullCoalescingOperatorTransformer(), null);
				variable.AcceptVisitor(new DynamicExtensionMethodsTranslator(), null);
				variable.AcceptVisitor(new TransformDynamicLambdaExpressions(), null);
                variable.AcceptVisitor(new TransformDynamicInvocationExpressions(), null);
				variable.AcceptVisitor(new TransformObsoleteMethods(), null);

				var expressionBody = GetAnonymousCreateExpression(lambdaExpression.Body);

				var anonymousTypeCreateExpression = expressionBody as AnonymousTypeCreateExpression;
				if (anonymousTypeCreateExpression == null && requiresSelectNewAnonymousType)
					throw new InvalidOperationException("Variable initializer select must have a lambda expression with an object create expression");

				var objectCreateExpression = expressionBody as ObjectCreateExpression;
				if (objectCreateExpression != null && requiresSelectNewAnonymousType)
					throw new InvalidOperationException("Variable initializer select must have a lambda expression creating an anonymous type but returning " + objectCreateExpression.Type);

				return variable;
			}
			catch (Exception e)
			{
				throw new InvalidOperationException("Could not understand query: " + e.Message, e);
			}
		}

		private static string ToQueryStatement(string query)
		{
			query = query.Replace("new() {", "new {").Replace("new () {", "new {"); ;
			if (query.EndsWith(";"))
				return "var qD1266A5B_A4BE_4108_BA29_79920DBC1308 = " + query;
			return "var qD1266A5B_A4BE_4108_BA29_79920DBC1308 = " + query + ";";
		}

		public static INode GetAnonymousCreateExpression(INode expression)
		{
			var invocationExpression = expression as InvocationExpression;

			if (invocationExpression == null)
				return expression;
			var memberReferenceExpression = invocationExpression.Target as MemberReferenceExpression;
			if (memberReferenceExpression == null)
				return expression;

			var typeReference = memberReferenceExpression.Target as TypeReferenceExpression;
			if (typeReference == null)
			{
				var objectCreateExpression = memberReferenceExpression.Target as AnonymousTypeCreateExpression;
				if (objectCreateExpression != null && memberReferenceExpression.MemberName == "Boost")
				{
					return objectCreateExpression;
				}
				return expression;
			}

			var simpleType = typeReference.Type as SimpleType;
			if (simpleType != null && simpleType.Identifier != "Raven.Database.Linq.PrivateExtensions.DynamicExtensionMethods")
				return expression;

			switch (memberReferenceExpression.MemberName)
			{
				case "Boost":
					return invocationExpression.Arguments.First();
			}
			return expression;
		}

		[CLSCompliant(false)]
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

		public static Type Compile(string source, string name, string queryText, OrderedPartCollection<AbstractDynamicCompilationExtension> extensions, string basePath, InMemoryRavenConfiguration configuration)
		{
			source = source.Replace("AbstractIndexCreationTask.SpatialGenerate", "SpatialGenerate"); // HACK, should probably be on the client side
			var indexCacheDir = GetIndexCacheDir(configuration);
			if (Directory.Exists(indexCacheDir) == false)
			{
				Directory.CreateDirectory(indexCacheDir);
			}

			var indexFilePath = GetIndexFilePath(source, indexCacheDir);

			var shouldCachedIndexBeRecompiled = ShouldIndexCacheBeRecompiled(indexFilePath);
			if (shouldCachedIndexBeRecompiled == false)
			{
				// Look up the index in the in-memory cache.
				CacheEntry entry;
				if (cacheEntries.TryGetValue(source, out entry))
				{
					Interlocked.Increment(ref entry.Usages);
					return entry.Type;
				}

				Type type;

				if (TryGetDiskCacheResult(source, name, configuration, indexFilePath, out type))
					return type;
			}

			var result = DoActualCompilation(source, name, queryText, extensions, basePath, indexFilePath, configuration);

			// ReSharper disable once RedundantArgumentName
			AddResultToCache(source, result, shouldUpdateIfExists: shouldCachedIndexBeRecompiled);

			return result;
		}

		private static bool ShouldIndexCacheBeRecompiled(string indexFilePath)
		{
			var ravenDatabaseFileInfo = new FileInfo(typeof(IndexDefinitionStorage).Assembly.Location);
			var indexFileInfo = new FileInfo(indexFilePath);
			if (indexFileInfo.Exists == false)
				return true;

			return DateTime.Compare(ravenDatabaseFileInfo.LastWriteTimeUtc, indexFileInfo.LastWriteTimeUtc) > 0;
		}

		private static void AddResultToCache(string source, Type result, bool shouldUpdateIfExists = false)
		{
			var cacheEntry = new CacheEntry
			{
				Source = source,
				Type = result,
				Usages = 1
			};

			if (shouldUpdateIfExists)
				cacheEntries.AddOrUpdate(source, cacheEntry, (key, existingValue) => cacheEntry);
			else
				cacheEntries.TryAdd(source, cacheEntry);

			if (cacheEntries.Count > 256)
			{
				var kvp = cacheEntries.OrderBy(x => x.Value.Usages).FirstOrDefault();
				if (kvp.Key != null)
				{
					CacheEntry _;
					cacheEntries.TryRemove(kvp.Key, out _);
				}
			}
		}

		private static bool TryGetDiskCacheResult(string source, string name, InMemoryRavenConfiguration configuration, string indexFilePath,
												  out Type type)
		{
			// It's not in the in-memory cache. See if it's been cached on disk.
			//
			// Q. Why do we cache on disk?
			// A. It decreases the duration of individual test runs. Instead of  
			//    recompiling the index each test run, we can just load them from disk.
			//    It also decreases creation time for indexes that were 
			//    previously created and deleted, affecting both production and test environments.
			//
			// For more info, see http://ayende.com/blog/161218/robs-sprint-idly-indexing?key=f37cf4dc-0e5c-43be-9b27-632f61ba044f#comments-form-location
			var indexCacheDir = GetIndexCacheDir(configuration);

			try
			{
				if (Directory.Exists(indexCacheDir) == false)
					Directory.CreateDirectory(indexCacheDir);
				type = TryGetIndexFromDisk(indexFilePath, name);
			}
			catch (UnauthorizedAccessException)
			{
				// permission issues
				type = null;
				return false;
			}
			catch (IOException)
			{
				// permission issues, probably
				type = null;
				return false;
			}

			if (type != null)
			{
				cacheEntries.TryAdd(source, new CacheEntry
				{
					Source = source,
					Type = type,
					Usages = 1
				});
				{
					return true;
				}
			}
			return false;
		}

		private static string GetIndexFilePath(string source, string indexCacheDir)
		{
			var hash = Encryptor.Current.Hash.Compute16(Encoding.UTF8.GetBytes(source));
			var sourceHashed = MonoHttpUtility.UrlEncode(Convert.ToBase64String(hash));
			var indexFilePath = Path.Combine(indexCacheDir,
				IndexingUtil.StableInvariantIgnoreCaseStringHash(source) + "." + sourceHashed + "." +
				(Debugger.IsAttached ? "debug" : "nodebug") + ".dll");
			return indexFilePath;
		}

		private static string GetIndexCacheDir(InMemoryRavenConfiguration configuration)
		{
			var indexCacheDir = configuration.CompiledIndexCacheDirectory;
			if (string.IsNullOrWhiteSpace(indexCacheDir))
				indexCacheDir = Path.Combine(Path.GetTempPath(), "Raven", "CompiledIndexCache");

			if (configuration.RunInMemory == false)
			{
				// if we aren't running in memory, we might be running in a mode where we can't write to our base directory
				// which is where we _want_ to write. In that case, our cache is going to be the db directory, instead, since 
				// we know we can write there
				try
				{
					if (Directory.Exists(indexCacheDir) == false)
						Directory.CreateDirectory(indexCacheDir);
					var touchFile = Path.Combine(indexCacheDir, Guid.NewGuid() + ".temp");
					File.WriteAllText(touchFile, "test that we can write to this path");
					File.Delete(touchFile);
					return indexCacheDir;
				}
				catch (Exception)
				{
				}

                indexCacheDir = Path.Combine(configuration.IndexStoragePath, "Raven", "CompiledIndexCache");
                if (Directory.Exists(indexCacheDir) == false)
                    Directory.CreateDirectory(indexCacheDir);
			}

			return indexCacheDir;
		}

		private static Type DoActualCompilation(string source, string name, string queryText, OrderedPartCollection<AbstractDynamicCompilationExtension> extensions,
												string basePath, string indexFilePath, InMemoryRavenConfiguration configuration)
		{
			var provider = new CSharpCodeProvider(new Dictionary<string, string> { { "CompilerVersion", "v4.0" } });

			var assemblies = new HashSet<string>
			{
				typeof (SystemTime).Assembly.Location,
				typeof (AbstractViewGenerator).Assembly.Location,
				typeof (NameValueCollection).Assembly.Location,
				typeof (Enumerable).Assembly.Location,
				typeof (Binder).Assembly.Location,
                AssemblyExtractor.GetExtractedAssemblyLocationFor(typeof(Field), configuration),
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
				GenerateInMemory = false,
				IncludeDebugInformation = Debugger.IsAttached,
				OutputAssembly = indexFilePath
			};
			if (basePath != null)
				compilerParameters.TempFiles = new TempFileCollection(basePath, false);

			foreach (var assembly in assemblies)
			{
				compilerParameters.ReferencedAssemblies.Add(assembly);
			}

			CompilerResults compileAssemblyFromFile;
			if (indexFilePath != null)
			{
				var sourceFileName = indexFilePath + ".cs";
				File.WriteAllText(sourceFileName, source);
				compileAssemblyFromFile = provider.CompileAssemblyFromFile(compilerParameters, sourceFileName);
			}
			else
			{
				compileAssemblyFromFile = provider.CompileAssemblyFromSource(compilerParameters, source);
			}
			var results = compileAssemblyFromFile;

			if (results.Errors.HasErrors)
			{
				var sb = new StringBuilder()
					.AppendLine("Compilation Errors:")
					.AppendLine();

				foreach (CompilerError error in results.Errors)
				{
					sb.AppendFormat("Line {0}, Position {1}: Error {2} - {3}\n", error.Line, error.Column, error.ErrorNumber, error.ErrorText);
				}

				sb.AppendLine();

				sb.AppendLine("Source code:")
				  .AppendLine(queryText)
				  .AppendLine();

				throw new InvalidOperationException(sb.ToString());
			}

			var asm = Assembly.Load(File.ReadAllBytes(indexFilePath)); // avoid locking the file

			// ReSharper disable once AssignNullToNotNullAttribute
			File.SetCreationTime(indexFilePath, DateTime.UtcNow);

			CodeVerifier.AssertNoSecurityCriticalCalls(asm);

			Type result = asm.GetType(name);
			if (result == null)
				throw new InvalidOperationException(
					"Could not get compiled index type. This probably means that there is something wrong with the assembly load context.");
			return result;
		}

		private static Type TryGetIndexFromDisk(string indexFilePath, string typeName)
		{
			try
			{
				if (File.Exists(indexFilePath))
				{
					// we don't use LoadFrom to avoid locking the file
					return System.Reflection.Assembly.Load(File.ReadAllBytes(indexFilePath)).GetType(typeName);
				}
			}
			catch
			{
				// If there were any problems loading this index from disk,
				// just delete it if we can. It will be regenerated later.
				try { File.Delete(indexFilePath); }
				catch { }
			}

			return null;
		}
	}
}
