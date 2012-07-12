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
using ICSharpCode.NRefactory.Ast;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Extensions;
using System.Linq;
using Raven.Database.Linq.Ast;
using Raven.Database.Plugins;

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
			:this(name, indexDefinition, new OrderedPartCollection<AbstractDynamicCompilationExtension>(), basePath, new InMemoryRavenConfiguration())
		{}

		public DynamicViewCompiler(string name, IndexDefinition indexDefinition, OrderedPartCollection<AbstractDynamicCompilationExtension> extensions, string basePath, InMemoryRavenConfiguration configuration)
		{
			this.indexDefinition = indexDefinition;
			this.extensions = extensions;
			if (configuration.RunInMemory == false)
			{
				this.basePath = Path.Combine(basePath, "TemporaryIndexDefinitionsAsSource");
				if (Directory.Exists(this.basePath) == false)
					Directory.CreateDirectory(this.basePath);
			}
			this.name = MonoHttpUtility.UrlEncode(name);
		    RequiresSelectNewAnonymousType = true;
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
			
			CSharpSafeName = "Index_"+ Regex.Replace(Name, @"[^\w\d]", "_");
			var type = new TypeDeclaration(Modifiers.Public, new List<AttributeSection>())
			{
				BaseTypes =
					{
						new TypeReference("AbstractViewGenerator")
					},
				Name = CSharpSafeName,
				Type = ClassType.Class
			};

			var ctor = new ConstructorDeclaration(CSharpSafeName,
			                                      Modifiers.Public,
			                                      new List<ParameterDeclarationExpression>(), null);
			type.Children.Add(ctor);
			ctor.Body = new BlockStatement();
		
			// this.ViewText = "96E65595-1C9E-4BFB-A0E5-80BF2D6FC185"; // Will be replaced later
			ctor.Body.AddChild(new ExpressionStatement(
			                   	new AssignmentExpression(
			                   		new MemberReferenceExpression(new ThisReferenceExpression(), "ViewText"),
			                   		AssignmentOperatorType.Assign,
			                   		new PrimitiveExpression(mapReduceTextToken, mapReduceTextToken))));

			foreach (var map in indexDefinition.Maps)
			{
				HandleMapFunction(ctor, map);
			}

			HandleTransformResults(ctor);

			HandleReduceDefintion(ctor);

		    AddAdditionalInformation(ctor);

			CompiledQueryText = QueryParsingUtils.GenerateText(type, extensions);
			var sb = new StringBuilder("@\"");
			foreach (var map in indexDefinition.Maps)
			{
				sb.AppendLine(map.Replace("\"", "\"\""));
			}
			if (indexDefinition.Reduce != null)
			{
				sb.AppendLine(indexDefinition.Reduce.Replace("\"", "\"\"")).AppendLine();
			}

			if (indexDefinition.TransformResults != null)
			{
				sb.AppendLine(indexDefinition.TransformResults.Replace("\"", "\"\"")).AppendLine();
			}

			sb.Append("\"");
			CompiledQueryText = CompiledQueryText.Replace("\"" + mapReduceTextToken + "\"",
			                                              sb.ToString());
		}

		private bool firstMap = true;
		private void HandleMapFunction(ConstructorDeclaration ctor, string map)
		{
			string entityName;

			VariableDeclaration mapDefinition = map.Trim().StartsWith("from") ? 
				TransformMapDefinitionFromLinqQuerySyntax(map, out entityName) : 
				TransformMapDefinitionFromLinqMethodSyntax(map, out entityName);

			if (string.IsNullOrEmpty(entityName) == false)
			{
				//this.ForEntityNames.Add(entityName);
				ctor.Body.AddChild(new ExpressionStatement(
				                   	new InvocationExpression(
				                   		new MemberReferenceExpression(
				                   			new MemberReferenceExpression(new ThisReferenceExpression(), "ForEntityNames"), "Add"),
				                   		new List<Expression> {new PrimitiveExpression(entityName, entityName)})
				                   	));
			}
			// this.AddMapDefinition(from doc in docs ...);
			ctor.Body.AddChild(new ExpressionStatement(
			                   	new InvocationExpression(new MemberReferenceExpression(new ThisReferenceExpression(), "AddMapDefinition"),
			                   	                         new List<Expression>{new LambdaExpression
			                   	                         {
			                   	                         	Parameters =
			                   	                         		{
			                   	                         			new ParameterDeclarationExpression(null, "docs")
			                   	                         		},
			                   	                         	ExpressionBody = mapDefinition.Initializer
			                   	                         }}
			                   		)));


			if(firstMap)
			{
				mapDefinition.Initializer.AcceptVisitor(captureSelectNewFieldNamesVisitor, null);
				firstMap = false;
			}
			else
			{
				var secondMapFieldNames = new CaptureSelectNewFieldNamesVisitor();
				mapDefinition.Initializer.AcceptVisitor(secondMapFieldNames, null);
				if(secondMapFieldNames.FieldNames.SetEquals(captureSelectNewFieldNamesVisitor.FieldNames) == false)
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

			mapDefinition.Initializer.AcceptVisitor(new ThrowOnInvalidMethodCalls(), null);
			mapDefinition.Initializer.AcceptVisitor(captureQueryParameterNamesVisitorForMap, null);
		}

		private void HandleTransformResults(ConstructorDeclaration ctor)
		{
			if (string.IsNullOrEmpty(indexDefinition.TransformResults)) 
				return;

			VariableDeclaration translatorDeclaration;
				
			if (indexDefinition.TransformResults.Trim().StartsWith("from"))
			{
				translatorDeclaration = QueryParsingUtils.GetVariableDeclarationForLinqQuery(indexDefinition.TransformResults, requiresSelectNewAnonymousType:false);
			}
			else
			{
				translatorDeclaration = QueryParsingUtils.GetVariableDeclarationForLinqMethods(indexDefinition.TransformResults,requiresSelectNewAnonymousType: false);
			}


			// this.Translator = (Database,results) => from doc in results ...;
			ctor.Body.AddChild(new ExpressionStatement(
			                   	new AssignmentExpression(
			                   		new MemberReferenceExpression(new ThisReferenceExpression(), "TransformResultsDefinition"),
			                   		AssignmentOperatorType.Assign,
			                   		new LambdaExpression
			                   		{
			                   			Parameters =
			                   				{
			                   					new ParameterDeclarationExpression(null, "Database"),
			                   					new ParameterDeclarationExpression(null, "results")
			                   				},
			                   			ExpressionBody = translatorDeclaration.Initializer
			                   		})));
		}

		private void HandleReduceDefintion(ConstructorDeclaration ctor)
		{
			if (!indexDefinition.IsMapReduce) 
				return;
			VariableDeclaration reduceDefiniton;
			Expression groupBySource;
			string groupByParamter;
			if (indexDefinition.Reduce.Trim().StartsWith("from"))
			{
				reduceDefiniton = QueryParsingUtils.GetVariableDeclarationForLinqQuery(indexDefinition.Reduce, RequiresSelectNewAnonymousType);
				var sourceSelect = (QueryExpression)((QueryExpression)reduceDefiniton.Initializer).FromClause.InExpression;
				groupBySource = ((QueryExpressionGroupClause)sourceSelect.SelectOrGroupClause).GroupBy;
				groupByParamter = sourceSelect.FromClause.Identifier;
			}
			else
			{
				reduceDefiniton = QueryParsingUtils.GetVariableDeclarationForLinqMethods(indexDefinition.Reduce, RequiresSelectNewAnonymousType);
				var invocation = ((InvocationExpression) reduceDefiniton.Initializer);
				var target = (MemberReferenceExpression) invocation.TargetObject;
				while(target.MemberName!="GroupBy")
				{
					invocation = (InvocationExpression) target.TargetObject;
					target = (MemberReferenceExpression)invocation.TargetObject;
				}
				var lambdaExpression = GetLambdaExpression(invocation);
				groupByParamter = lambdaExpression.Parameters[0].ParameterName;
				groupBySource = lambdaExpression.ExpressionBody;
			}

			var mapFields = captureSelectNewFieldNamesVisitor.FieldNames.ToList();
			captureSelectNewFieldNamesVisitor.Clear();// reduce override the map fields
			reduceDefiniton.Initializer.AcceptVisitor(captureSelectNewFieldNamesVisitor, null);
			reduceDefiniton.Initializer.AcceptChildren(captureQueryParameterNamesVisitorForReduce, null);
			reduceDefiniton.Initializer.AcceptVisitor(new ThrowOnInvalidMethodCalls(), null);

			ValidateMapReduceFields(mapFields);

			// this.ReduceDefinition = from result in results...;
			ctor.Body.AddChild(new ExpressionStatement(
			                   	new AssignmentExpression(
			                   		new MemberReferenceExpression(new ThisReferenceExpression(),
			                   		                              "ReduceDefinition"),
			                   		AssignmentOperatorType.Assign,
			                   		new LambdaExpression
			                   		{
			                   			Parameters =
			                   				{
			                   					new ParameterDeclarationExpression(null, "results")
			                   				},
			                   			ExpressionBody = reduceDefiniton.Initializer
			                   		})));
				
			ctor.Body.AddChild(new ExpressionStatement(
			                   	new AssignmentExpression(
			                   		new MemberReferenceExpression(new ThisReferenceExpression(),
			                   		                              "GroupByExtraction"),
			                   		AssignmentOperatorType.Assign,
			                   		new LambdaExpression
			                   		{
			                   			Parameters =
			                   				{
			                   					new ParameterDeclarationExpression(null, groupByParamter)
			                   				},
			                   			ExpressionBody = groupBySource
			                   		})));
		}

		private static LambdaExpression GetLambdaExpression(InvocationExpression invocation)
		{
			var expression = invocation.Arguments[0];
			var castExpression = expression as CastExpression;
			if(castExpression != null)
			{
				expression = castExpression.Expression;
			}
			var parenthesizedExpression = expression as ParenthesizedExpression;
			if(parenthesizedExpression != null)
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
				ctor.Body.AddChild(
					new ExpressionStatement(
						new InvocationExpression(
							new MemberReferenceExpression(
								new ThisReferenceExpression(),
								methodToCall
								),
							new List<Expression> { new PrimitiveExpression(fieldName, fieldName) }
							)
						)
					);
			}
		}

		public bool RequiresSelectNewAnonymousType { get; set; }

		private VariableDeclaration TransformMapDefinitionFromLinqMethodSyntax(string query, out string entityName)
		{
			var variableDeclaration = QueryParsingUtils.GetVariableDeclarationForLinqMethods(query, RequiresSelectNewAnonymousType);
			AddEntityNameFilteringIfNeeded(variableDeclaration, out entityName);

			variableDeclaration.AcceptVisitor(new AddDocumentIdToLambdas(), null);
			return variableDeclaration;
		}

		public class AddDocumentIdToLambdas : ICSharpCode.NRefactory.Visitors.AbstractAstTransformer
		{
			public override object VisitLambdaExpression(LambdaExpression lambdaExpression, object data)
			{
				if (AddDocumentIdFieldToLambdaIfCreatingNewObject(lambdaExpression))
					return null;
				return base.VisitLambdaExpression(lambdaExpression, data);
			}

			private bool AddDocumentIdFieldToLambdaIfCreatingNewObject(LambdaExpression lambdaExpression)
			{
				var objectCreateExpression = QueryParsingUtils.GetAnonymousCreateExpression(lambdaExpression.ExpressionBody) as ObjectCreateExpression;

				if (objectCreateExpression == null || objectCreateExpression.IsAnonymousType == false)
					return false;

				var objectInitializer = objectCreateExpression.ObjectInitializer;

				var identifierExpression = new IdentifierExpression(lambdaExpression.Parameters[0].ParameterName);

				if (objectInitializer.CreateExpressions.OfType<NamedArgumentExpression>().Any(x => x.Name == Constants.DocumentIdFieldName))
					return false;


				objectCreateExpression.ObjectInitializer = new CollectionInitializerExpression(objectInitializer.CreateExpressions.ToList())
				{
					CreateExpressions =
						{
							new NamedArgumentExpression
							{
								Name = Constants.DocumentIdFieldName,
								Expression = new MemberReferenceExpression(identifierExpression, Constants.DocumentIdFieldName)
							}
						}
				};

				return true;
			}

		}

		private void AddEntityNameFilteringIfNeeded(VariableDeclaration variableDeclaration, out string entityName)
		{
			entityName = null;
			var invocationExpression = ((InvocationExpression)variableDeclaration.Initializer);
			var targetExpression = ((MemberReferenceExpression)invocationExpression.TargetObject);
			while (targetExpression.TargetObject is InvocationExpression)
			{
				invocationExpression = (InvocationExpression) targetExpression.TargetObject;
				targetExpression = (MemberReferenceExpression)invocationExpression.TargetObject;
			}
			if (targetExpression.TargetObject is MemberReferenceExpression) // collection
			{
				var mre = (MemberReferenceExpression)targetExpression.TargetObject;
				entityName = mre.MemberName;
				//doc["@metadata"]["Raven-Entity-Name"]
				var metadata = new IndexerExpression(
					new IndexerExpression(new IdentifierExpression("__document"), new List<Expression> { new PrimitiveExpression("@metadata", "@metadata") }),
					new List<Expression> { new PrimitiveExpression(Constants.RavenEntityName, Constants.RavenEntityName) }
					);

				// string.Equals(doc["@metadata"]["Raven-Entity-Name"], "Blogs", StringComparison.InvariantCultureIgnoreCase)
				var binaryOperatorExpression =
					new InvocationExpression(
						new MemberReferenceExpression(new TypeReferenceExpression(new TypeReference("string", true)), "Equals"),
						new List<Expression>
						{
							metadata,
							new PrimitiveExpression(mre.MemberName, mre.MemberName),
							new MemberReferenceExpression(new TypeReferenceExpression(new TypeReference(typeof(StringComparison).FullName)),"InvariantCultureIgnoreCase")
						});
				var whereMethod = new InvocationExpression(new MemberReferenceExpression(mre.TargetObject, "Where"),
				                                           new List<Expression>
				                                           {
				                                           	new LambdaExpression
				                                           	{
				                                           		Parameters =
				                                           			{
				                                           				new ParameterDeclarationExpression(null, "__document")
				                                           			},
				                                           		ExpressionBody = binaryOperatorExpression
				                                           	}
				                                           });

				invocationExpression.TargetObject = new MemberReferenceExpression(whereMethod, targetExpression.MemberName);

			}
		}


		private VariableDeclaration TransformMapDefinitionFromLinqQuerySyntax(string query, out string entityName)
		{
			entityName = null;
			var variableDeclaration = QueryParsingUtils.GetVariableDeclarationForLinqQuery(query, RequiresSelectNewAnonymousType);
			var queryExpression = ((QueryExpression) variableDeclaration.Initializer);
			var expression = queryExpression.FromClause.InExpression;
			if(expression is MemberReferenceExpression) // collection
			{
				var mre = (MemberReferenceExpression)expression;
				entityName = mre.MemberName;
				queryExpression.FromClause.InExpression = mre.TargetObject;
				//doc["@metadata"]["Raven-Entity-Name"]
				var metadata = new IndexerExpression(
					new IndexerExpression(new IdentifierExpression(queryExpression.FromClause.Identifier), new List<Expression> { new PrimitiveExpression("@metadata", "@metadata") }),
					new List<Expression> { new PrimitiveExpression(Constants.RavenEntityName, Constants.RavenEntityName) }
					);

				// string.Equals(doc["@metadata"]["Raven-Entity-Name"], "Blogs", StringComparison.InvariantCultureIgnoreCase)
				var binaryOperatorExpression =
					new InvocationExpression(
						new MemberReferenceExpression(new TypeReferenceExpression(new TypeReference("string", true)), "Equals"),
						new List<Expression>
						{
							metadata,
							new PrimitiveExpression(mre.MemberName, mre.MemberName),
							new MemberReferenceExpression(new TypeReferenceExpression(new TypeReference(typeof(StringComparison).FullName)),"InvariantCultureIgnoreCase")
						});
				queryExpression.MiddleClauses.Insert(0,
				                                     new QueryExpressionWhereClause
				                                     {
														 Condition = binaryOperatorExpression
				                                     });
			}
			var selectOrGroupClause = queryExpression.SelectOrGroupClause;
			var projection = ((QueryExpressionSelectClause) selectOrGroupClause).Projection;
			if(projection is ObjectCreateExpression == false)
				return variableDeclaration;

			var objectInitializer = ((ObjectCreateExpression) projection).ObjectInitializer;

			var identifierExpression = new IdentifierExpression(queryExpression.FromClause.Identifier);

			if (objectInitializer.CreateExpressions.OfType<NamedArgumentExpression>().Any(x => x.Name == Constants.DocumentIdFieldName))
				return variableDeclaration;

			objectInitializer.CreateExpressions.Add(
				new NamedArgumentExpression
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

			var abstractViewGenerator = (AbstractViewGenerator) Activator.CreateInstance(GeneratedType);
			abstractViewGenerator.SourceCode = CompiledQueryText;
			return abstractViewGenerator;
		}
	}
}
