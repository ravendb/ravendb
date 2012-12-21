//-----------------------------------------------------------------------
// <copyright file="DynamicViewCompiler.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using ICSharpCode.NRefactory;
using ICSharpCode.NRefactory.CSharp;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Extensions;
using System.Linq;
using Raven.Database.Linq.Ast;
using Raven.Database.Plugins;
using Raven.Database.Util;

namespace Raven.Database.Linq
{
	/// <summary>
	/// 	Takes two query expressions as strings, and compile them.
	/// 	Along the way we apply some minimal transformations, the end result is an instance
	/// 	of AbstractViewGenerator, representing the map/reduce functions
	/// </summary>
	public class DynamicViewCompiler
	{
		private readonly IndexDefinition indexDefinition;
		private readonly OrderedPartCollection<AbstractDynamicCompilationExtension> extensions;
		private readonly string basePath;
		private const string mapReduceTextToken = "96E65595-1C9E-4BFB-A0E5-80BF2D6FC185";

		private readonly string name;
		private readonly CaptureSelectNewFieldNamesVisitor captureSelectNewFieldNamesVisitor = new CaptureSelectNewFieldNamesVisitor();
		private readonly CaptureQueryParameterNamesVisitor captureQueryParameterNamesVisitorForMap = new CaptureQueryParameterNamesVisitor();
		private readonly CaptureQueryParameterNamesVisitor captureQueryParameterNamesVisitorForReduce = new CaptureQueryParameterNamesVisitor();

		public DynamicViewCompiler(string name, IndexDefinition indexDefinition, string basePath)
			: this(name, indexDefinition, new OrderedPartCollection<AbstractDynamicCompilationExtension>(), basePath, new InMemoryRavenConfiguration())
		{ }

		public DynamicViewCompiler(string name, IndexDefinition indexDefinition, OrderedPartCollection<AbstractDynamicCompilationExtension> extensions, string basePath, InMemoryRavenConfiguration configuration)
		{
			this.indexDefinition = indexDefinition;
			this.extensions = extensions;
			if (configuration.RunInMemory == false)
			{
				this.basePath = Path.Combine(basePath, "TemporaryIndexDefinitionsAsSource");
				if (Directory.Exists(this.basePath) == false)
				{
					Directory.CreateDirectory(this.basePath);
				}
				else
				{
					MaybeCleanupDirectory(this.basePath);
				}
			}
			this.name = MonoHttpUtility.UrlEncode(name);
			RequiresSelectNewAnonymousType = true;
		}

		private static readonly ConcurrentSet<string> paths = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		private static void MaybeCleanupDirectory(string path)
		{
			if (paths.TryAdd(path) == false)
				return;

			foreach (var file in Directory.GetFiles(path, "*.dll"))
			{
				try
				{
					File.Delete(file);
				}
				catch (Exception)
				{
					// failure here is expected, this is probably another index that is currently
					// live that is locking the file, we will get it next restart
				}
			}
		}

		public string CompiledQueryText { get; set; }
		public Type GeneratedType { get; set; }

		public string Name
		{
			get { return name; }
		}

		public string CSharpSafeName { get; set; }

		private void TransformQueryToClass()
		{

			CSharpSafeName = "Index_" + Regex.Replace(Name, @"[^\w\d]", "_");
			var type = new TypeDeclaration
			{
				Modifiers = Modifiers.Public,
				BaseTypes =
					{
						new SimpleType(typeof(AbstractViewGenerator).FullName)
					},
				Name = CSharpSafeName,
				ClassType = ClassType.Class
			};


			var body = new BlockStatement();

			// this.ViewText = "96E65595-1C9E-4BFB-A0E5-80BF2D6FC185"; // Will be replaced later
			var viewText = new ExpressionStatement(
				new AssignmentExpression(
					new MemberReferenceExpression(new ThisReferenceExpression(), "ViewText"),
					AssignmentOperatorType.Assign,
					new StringLiteralExpression(mapReduceTextToken)));
			body.Statements.Add(viewText);

			var ctor = new ConstructorDeclaration
			{
				Name = CSharpSafeName,
				Modifiers = Modifiers.Public,
				Body = body
			};
			type.Members.Add(ctor);
			foreach (var map in indexDefinition.Maps)
			{
				HandleMapFunction(ctor, map);
			}

			HandleTransformResults(ctor);

			HandleReduceDefinition(ctor);

			AddAdditionalInformation(ctor);

			CompiledQueryText = QueryParsingUtils.GenerateText(type, extensions);
			var sb = new StringBuilder("@\"");

			foreach (var map in indexDefinition.Maps)
			{
				sb.AppendLine(map.Replace("\"", "\"\""));
			}

			if (indexDefinition.Reduce != null)
			{
				sb.AppendLine(indexDefinition.Reduce.Replace("\"", "\"\""));
			}

			if (indexDefinition.TransformResults != null)
			{
				sb.AppendLine(indexDefinition.TransformResults.Replace("\"", "\"\""));
			}
			sb.Length = sb.Length - 2;

			sb.Append("\"");
			CompiledQueryText = CompiledQueryText.Replace('"' + mapReduceTextToken + '"', sb.ToString());
		}

		private bool firstMap = true;
		private void HandleMapFunction(ConstructorDeclaration ctor, string map)
		{
			string entityName;

			VariableInitializer mapDefinition = map.Trim().StartsWith("from") ?
				TransformMapDefinitionFromLinqQuerySyntax(map, out entityName) :
				TransformMapDefinitionFromLinqMethodSyntax(map, out entityName);

			if (string.IsNullOrEmpty(entityName) == false)
			{
				//this.ForEntityNames.Add(entityName);
				ctor.Body.Statements.Add(new ExpressionStatement(
									new InvocationExpression(
										new MemberReferenceExpression(
											new MemberReferenceExpression(new ThisReferenceExpression(), "ForEntityNames"), "Add"),
										new List<Expression> { new StringLiteralExpression(entityName) })
									));
			}
			// this.AddMapDefinition(from doc in docs ...);
			ctor.Body.Statements.Add(new ExpressionStatement(
								   new InvocationExpression(
									   new MemberReferenceExpression(new ThisReferenceExpression(), "AddMapDefinition"),
									   new List<Expression>
					                   {
						                   new LambdaExpression
						                   {
							                   Parameters =
							                   {
								                   new ParameterDeclaration(null, "docs")
							                   },
							                   Body = mapDefinition.Initializer.Clone()
						                   }
					                   }
									   )));


			if (firstMap)
			{
				mapDefinition.Initializer.AcceptVisitor(captureSelectNewFieldNamesVisitor, null);
				firstMap = false;
			}
			else
			{
				var secondMapFieldNames = new CaptureSelectNewFieldNamesVisitor();
				mapDefinition.Initializer.AcceptVisitor(secondMapFieldNames, null);
				if (secondMapFieldNames.FieldNames.SetEquals(captureSelectNewFieldNamesVisitor.FieldNames) == false)
				{
					var message = string.Format(@"Map functions defined as part of a multi map index must return identical types.
Baseline map		: {0}
Non matching map	: {1}

Common fields		: {2}
Missing fields		: {3}
Additional fields	: {4}", indexDefinition.Maps.First(),
												map,
												string.Join(", ", captureSelectNewFieldNamesVisitor.FieldNames.Intersect(secondMapFieldNames.FieldNames)),
												string.Join(", ", captureSelectNewFieldNamesVisitor.FieldNames.Except(secondMapFieldNames.FieldNames)),
												string.Join(", ", secondMapFieldNames.FieldNames.Except(captureSelectNewFieldNamesVisitor.FieldNames))
						);
					throw new InvalidOperationException(message);
				}
			}

			mapDefinition.Initializer.AcceptVisitor(new ThrowOnInvalidMethodCalls(null), null);
			mapDefinition.Initializer.AcceptVisitor(captureQueryParameterNamesVisitorForMap, null);
		}

		private void HandleTransformResults(ConstructorDeclaration ctor)
		{
			if (string.IsNullOrEmpty(indexDefinition.TransformResults))
				return;

			VariableInitializer translatorDeclaration;

			if (indexDefinition.TransformResults.Trim().StartsWith("from"))
			{
				translatorDeclaration = QueryParsingUtils.GetVariableDeclarationForLinqQuery(indexDefinition.TransformResults, requiresSelectNewAnonymousType: false);
			}
			else
			{
				translatorDeclaration = QueryParsingUtils.GetVariableDeclarationForLinqMethods(indexDefinition.TransformResults, requiresSelectNewAnonymousType: false);
			}

			translatorDeclaration.AcceptVisitor(new ThrowOnInvalidMethodCallsForTransformResults(), null);


			// this.Translator = (Database,results) => from doc in results ...;
			ctor.Body.Statements.Add(new ExpressionStatement(
								new AssignmentExpression(
									new MemberReferenceExpression(new ThisReferenceExpression(), "TransformResultsDefinition"),
									AssignmentOperatorType.Assign,
									new LambdaExpression
									{
										Parameters =
			                   				{
			                   					new ParameterDeclaration(null, "Database"),
			                   					new ParameterDeclaration(null, "results")
			                   				},
										Body = translatorDeclaration.Initializer.Clone()
									})));
		}

		private void HandleReduceDefinition(ConstructorDeclaration ctor)
		{
			if (!indexDefinition.IsMapReduce)
				return;
			VariableInitializer reduceDefiniton;
			AstNode groupBySource;
			string groupByParamter;
			string groupByIdentifier;
			if (indexDefinition.Reduce.Trim().StartsWith("from"))
			{
				reduceDefiniton = QueryParsingUtils.GetVariableDeclarationForLinqQuery(indexDefinition.Reduce, RequiresSelectNewAnonymousType);
				var queryExpression = ((QueryExpression)reduceDefiniton.Initializer);
				var queryContinuationClause = queryExpression.Clauses.OfType<QueryContinuationClause>().First();
				var queryGroupClause = queryContinuationClause.PrecedingQuery.Clauses.OfType<QueryGroupClause>().First();
				groupByIdentifier = queryContinuationClause.Identifier;
				groupBySource = queryGroupClause.Key;
				groupByParamter = queryContinuationClause.PrecedingQuery.Clauses.OfType<QueryFromClause>().First().Identifier;
			}
			else
			{
				reduceDefiniton = QueryParsingUtils.GetVariableDeclarationForLinqMethods(indexDefinition.Reduce, RequiresSelectNewAnonymousType);
				var initialInvocation = ((InvocationExpression)reduceDefiniton.Initializer);
				var invocation = initialInvocation;
				var target = (MemberReferenceExpression)invocation.Target;
				while (target.MemberName != "GroupBy")
				{
					invocation = (InvocationExpression)target.Target;
					target = (MemberReferenceExpression)invocation.Target;
				}
				var lambdaExpression = GetLambdaExpression(invocation);
				groupByParamter = lambdaExpression.Parameters.First().Name;
				groupBySource = lambdaExpression.Body;
				groupByIdentifier = null;
			}

			var mapFields = captureSelectNewFieldNamesVisitor.FieldNames.ToList();
			captureSelectNewFieldNamesVisitor.Clear();// reduce override the map fields
			reduceDefiniton.Initializer.AcceptVisitor(captureSelectNewFieldNamesVisitor, null);
			reduceDefiniton.Initializer.AcceptVisitor(captureQueryParameterNamesVisitorForReduce, null);
			reduceDefiniton.Initializer.AcceptVisitor(new ThrowOnInvalidMethodCalls(groupByIdentifier), null);

			ValidateMapReduceFields(mapFields);

			// this.ReduceDefinition = from result in results...;
			ctor.Body.Statements.Add(new ExpressionStatement(
								new AssignmentExpression(
									new MemberReferenceExpression(new ThisReferenceExpression(),
																  "ReduceDefinition"),
									AssignmentOperatorType.Assign,
									new LambdaExpression
									{
										Parameters =
			                   				{
			                   					new ParameterDeclaration(null, "results")
			                   				},
										Body = reduceDefiniton.Initializer.Clone()
									})));

			ctor.Body.Statements.Add(new ExpressionStatement(
								new AssignmentExpression(
									new MemberReferenceExpression(new ThisReferenceExpression(),
																  "GroupByExtraction"),
									AssignmentOperatorType.Assign,
									new LambdaExpression
									{
										Parameters =
			                   				{
			                   					new ParameterDeclaration(null, groupByParamter)
			                   				},
										Body = groupBySource.Clone()
									})));
		}

		private static LambdaExpression GetLambdaExpression(InvocationExpression invocation)
		{
			var expression = invocation.Arguments.First();
			var castExpression = expression as CastExpression;
			if (castExpression != null)
			{
				expression = castExpression.Expression;
			}
			var parenthesizedExpression = expression as ParenthesizedExpression;
			if (parenthesizedExpression != null)
			{
				expression = parenthesizedExpression.Expression;
			}
			return ((LambdaExpression)expression);
		}

		private void ValidateMapReduceFields(List<string> mapFields)
		{
			mapFields.Remove("__document_id");
			var reduceFields = captureSelectNewFieldNamesVisitor.FieldNames;
			if (reduceFields.SetEquals(mapFields) == false)
			{
				throw new InvalidOperationException(
					string.Format(
						@"The result type is not consistent across map and reduce:
Common fields: {0}
Map only fields   : {1}
Reduce only fields: {2}
",
						string.Join(", ", mapFields.Intersect(reduceFields).OrderBy(x => x)),
						string.Join(", ", mapFields.Except(reduceFields).OrderBy(x => x)),
						string.Join(", ", reduceFields.Except(mapFields).OrderBy(x => x))));
			}
		}

		private void AddAdditionalInformation(ConstructorDeclaration ctor)
		{
			AddInformation(ctor, captureSelectNewFieldNamesVisitor.FieldNames, "AddField");
			AddInformation(ctor, captureQueryParameterNamesVisitorForMap.QueryParameters, "AddQueryParameterForMap");
			AddInformation(ctor, captureQueryParameterNamesVisitorForMap.QueryParameters, "AddQueryParameterForReduce");
		}

		private static void AddInformation(ConstructorDeclaration ctor, HashSet<string> fieldNames, string methodToCall)
		{
			foreach (var fieldName in fieldNames)
			{
				ctor.Body.Statements.Add(
					new ExpressionStatement(
						new InvocationExpression(
							new MemberReferenceExpression(
								new ThisReferenceExpression(),
								methodToCall
								),
							new List<Expression> { new StringLiteralExpression(fieldName) }
							)
						));
			}
		}

		public bool RequiresSelectNewAnonymousType { get; set; }

		private VariableInitializer TransformMapDefinitionFromLinqMethodSyntax(string query, out string entityName)
		{
			var variableDeclaration = QueryParsingUtils.GetVariableDeclarationForLinqMethods(query, RequiresSelectNewAnonymousType);
			AddEntityNameFilteringIfNeeded(variableDeclaration, out entityName);

			variableDeclaration.AcceptVisitor(new AddDocumentIdToLambdas(), null);
			return variableDeclaration;
		}

		[CLSCompliant(false)]
		public class AddDocumentIdToLambdas : DepthFirstAstVisitor<object, object>
		{
			public override object VisitLambdaExpression(LambdaExpression lambdaExpression, object data)
			{
				if (AddDocumentIdFieldToLambdaIfCreatingNewObject(lambdaExpression))
					return null;
				return base.VisitLambdaExpression(lambdaExpression, data);
			}

			private bool AddDocumentIdFieldToLambdaIfCreatingNewObject(LambdaExpression lambdaExpression)
			{
				var objectCreateExpression = QueryParsingUtils.GetAnonymousCreateExpression(lambdaExpression.Body) as AnonymousTypeCreateExpression;

				if (objectCreateExpression == null)
					return false;

				var initializers = objectCreateExpression.Initializers;

				var identifierExpression = new IdentifierExpression(lambdaExpression.Parameters.First().Name);

				if (initializers.OfType<NamedExpression>().Any(x => x.Name == Constants.DocumentIdFieldName))
					return false;

				objectCreateExpression.Initializers.Add(new NamedExpression
				{
					Name = Constants.DocumentIdFieldName,
					Expression = new MemberReferenceExpression(identifierExpression, Constants.DocumentIdFieldName)
				});

				return true;
			}

		}

		private void AddEntityNameFilteringIfNeeded(VariableInitializer variableDeclaration, out string entityName)
		{
			entityName = null;
			var invocationExpression = ((InvocationExpression)variableDeclaration.Initializer);
			var targetExpression = ((MemberReferenceExpression)invocationExpression.Target);
			while (targetExpression.Target is InvocationExpression)
			{
				invocationExpression = (InvocationExpression)targetExpression.Target;
				targetExpression = (MemberReferenceExpression)invocationExpression.Target;
			}
			if (targetExpression.Target is MemberReferenceExpression) // collection
			{
				var mre = (MemberReferenceExpression)targetExpression.Target;
				entityName = mre.MemberName;
				//doc["@metadata"]["Raven-Entity-Name"]
				var metadata = new IndexerExpression(
					new IndexerExpression(new IdentifierExpression("__document"), new List<Expression> { new StringLiteralExpression("@metadata") }),
					new List<Expression> { new StringLiteralExpression(Constants.RavenEntityName) }
					);

				// string.Equals(doc["@metadata"]["Raven-Entity-Name"], "Blogs", StringComparison.InvariantCultureIgnoreCase)
				var binaryOperatorExpression =
					new InvocationExpression(
						new MemberReferenceExpression(new TypeReferenceExpression(new PrimitiveType("string")), "Equals"),
						new List<Expression>
						{
							metadata,
							new StringLiteralExpression(mre.MemberName),
							new MemberReferenceExpression(new TypeReferenceExpression(new SimpleType(typeof(StringComparison).FullName)),"InvariantCultureIgnoreCase")
						});
				var whereMethod = new InvocationExpression(new MemberReferenceExpression(mre.Target.Clone(), "Where"),
														   new List<Expression>
				                                           {
				                                           	new LambdaExpression
				                                           	{
				                                           		Parameters =
				                                           			{
				                                           				new ParameterDeclaration(null, "__document")
				                                           			},
				                                           		Body = binaryOperatorExpression.Clone()
				                                           	}
				                                           });

				invocationExpression.Target = new MemberReferenceExpression(whereMethod.Clone(), targetExpression.MemberName);

			}
		}


		private VariableInitializer TransformMapDefinitionFromLinqQuerySyntax(string query, out string entityName)
		{
			entityName = null;
			var variableDeclaration = QueryParsingUtils.GetVariableDeclarationForLinqQuery(query, RequiresSelectNewAnonymousType);
			var queryExpression = ((QueryExpression)variableDeclaration.Initializer);
			var fromClause = queryExpression.Clauses.OfType<QueryFromClause>().First();
			var expression = fromClause.Expression;
			if (expression is MemberReferenceExpression) // collection
			{
				var mre = (MemberReferenceExpression)expression;
				entityName = mre.MemberName;
				fromClause.Expression = mre.Target;
				//doc["@metadata"]["Raven-Entity-Name"]
				var metadata = new IndexerExpression(
					new IndexerExpression(new IdentifierExpression(fromClause.Identifier), new List<Expression> { new StringLiteralExpression("@metadata") }),
					new List<Expression> { new StringLiteralExpression(Constants.RavenEntityName) }
					);

				// string.Equals(doc["@metadata"]["Raven-Entity-Name"], "Blogs", StringComparison.InvariantCultureIgnoreCase)
				var binaryOperatorExpression =
					new InvocationExpression(
						new MemberReferenceExpression(new TypeReferenceExpression(new PrimitiveType("string")), "Equals"),
						new List<Expression>
						{
							metadata,
							new StringLiteralExpression(mre.MemberName),
							new MemberReferenceExpression(new TypeReferenceExpression(new SimpleType(typeof(StringComparison).FullName)),"InvariantCultureIgnoreCase")
						});

				queryExpression.Clauses.InsertAfter(fromClause,
													 new QueryWhereClause
													 {
														 Condition = binaryOperatorExpression
													 });
			}
			var projection = queryExpression.Clauses.OfType<QuerySelectClause>().First().Expression;
			if (projection is AnonymousTypeCreateExpression == false)
				return variableDeclaration;

			var objectInitializer = ((AnonymousTypeCreateExpression)projection).Initializers;

			var identifierExpression = new IdentifierExpression(fromClause.Identifier);

			if (objectInitializer.OfType<NamedExpression>().Any(x => x.Name == Constants.DocumentIdFieldName))
				return variableDeclaration;

			objectInitializer.Add(
				new NamedExpression
				{
					Name = Constants.DocumentIdFieldName,
					Expression = new MemberReferenceExpression(identifierExpression, Constants.DocumentIdFieldName)
				});
			return variableDeclaration;
		}

		public AbstractViewGenerator GenerateInstance()
		{
			TransformQueryToClass();

			GeneratedType = QueryParsingUtils.Compile(CompiledQueryText, CSharpSafeName, CompiledQueryText, extensions, basePath);

			var abstractViewGenerator = (AbstractViewGenerator)Activator.CreateInstance(GeneratedType);
			abstractViewGenerator.SourceCode = CompiledQueryText;
			abstractViewGenerator.Init(indexDefinition);
			return abstractViewGenerator;
		}
	}
}
