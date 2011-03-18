//-----------------------------------------------------------------------
// <copyright file="DynamicViewCompiler.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Web;
using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.PrettyPrinter;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using System.Linq;
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
		private readonly AbstractDynamicCompilationExtension[] extensions;
		private readonly string basePath;
		private const string mapReduceTextToken = "96E65595-1C9E-4BFB-A0E5-80BF2D6FC185";

		private readonly string name;
		private readonly CaptureSelectNewFieldNamesVisitor captureSelectNewFieldNamesVisitor = new CaptureSelectNewFieldNamesVisitor();
		private readonly CaptureQueryParameterNamesVisitor captureQueryParameterNamesVisitorForMap = new CaptureQueryParameterNamesVisitor();
		private readonly CaptureQueryParameterNamesVisitor captureQueryParameterNamesVisitorForReduce = new CaptureQueryParameterNamesVisitor();

		public DynamicViewCompiler(string name, IndexDefinition indexDefinition, AbstractDynamicCompilationExtension[] extensions, string basePath)
		{
			this.indexDefinition = indexDefinition;
			this.extensions = extensions;
			this.basePath = Path.Combine(basePath, "TemporaryIndexDefinitionsAsSource");
			if (Directory.Exists(this.basePath) == false)
				Directory.CreateDirectory(this.basePath);
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
			string entityName;
			var mapDefinition = TransformMapDefinition(out entityName);

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
			//this.ForEntityName = entityName;
			ctor.Body.AddChild(new ExpressionStatement(
								new AssignmentExpression(
									new MemberReferenceExpression(new ThisReferenceExpression(), "ForEntityName"),
									AssignmentOperatorType.Assign,
									new PrimitiveExpression(entityName, entityName))));

			// this.ViewText = "96E65595-1C9E-4BFB-A0E5-80BF2D6FC185"; // Will be replaced later
			ctor.Body.AddChild(new ExpressionStatement(
			                   	new AssignmentExpression(
			                   		new MemberReferenceExpression(new ThisReferenceExpression(), "ViewText"),
			                   		AssignmentOperatorType.Assign,
			                   		new PrimitiveExpression(mapReduceTextToken, mapReduceTextToken))));

			// this.MapDefinition = from doc in docs ...;
			ctor.Body.AddChild(new ExpressionStatement(
			                   	new AssignmentExpression(
			                   		new MemberReferenceExpression(new ThisReferenceExpression(), "MapDefinition"),
			                   		AssignmentOperatorType.Assign,
			                   		new LambdaExpression
			                   		{
			                   			Parameters =
			                   				{
			                   					new ParameterDeclarationExpression(null, "docs")
			                   				},
			                   			ExpressionBody = mapDefinition.Initializer
			                   		})));


			mapDefinition.Initializer.AcceptVisitor(captureSelectNewFieldNamesVisitor, null);

			mapDefinition.Initializer.AcceptVisitor(captureQueryParameterNamesVisitorForMap, null);

            HandleTransformResults(ctor);

			HandleReduceDefintion(ctor);

		    AddAdditionalInformation(ctor);

			CompiledQueryText = QueryParsingUtils.GenerateText(type, extensions);
			var compiledQueryText = "@\"" + indexDefinition.Map.Replace("\"", "\"\"");
			if (indexDefinition.Reduce != null)
			{
				compiledQueryText += Environment.NewLine + indexDefinition.Reduce.Replace("\"", "\"\"");
			}

            if (indexDefinition.TransformResults != null)
            {
                compiledQueryText += Environment.NewLine + indexDefinition.TransformResults.Replace("\"", "\"\"");
            }

			compiledQueryText += "\"";
			CompiledQueryText = CompiledQueryText.Replace("\"" + mapReduceTextToken + "\"",
			                                              compiledQueryText);
		}

		private void HandleTransformResults(ConstructorDeclaration ctor)
		{
			if (indexDefinition.TransformResults == null) 
				return;

			VariableDeclaration translatorDeclaration;
                
			if (indexDefinition.TransformResults.Trim().StartsWith("from"))
			{
				translatorDeclaration = QueryParsingUtils.GetVariableDeclarationForLinqQuery(indexDefinition.TransformResults, requiresSelectNewAnonymousType:false);
			}
			else
			{
				translatorDeclaration = QueryParsingUtils.GetVariableDeclarationForLinqMethods(indexDefinition.TransformResults);
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
				reduceDefiniton = QueryParsingUtils.GetVariableDeclarationForLinqMethods(indexDefinition.Reduce);
				var invocation = ((InvocationExpression) reduceDefiniton.Initializer);
				var target = (MemberReferenceExpression) invocation.TargetObject;
				while(target.MemberName!="GroupBy")
				{
					invocation = (InvocationExpression) target.TargetObject;
					target = (MemberReferenceExpression)invocation.TargetObject;
				}
				var lambdaExpression = ((LambdaExpression)invocation.Arguments[0]);
				groupByParamter = lambdaExpression.Parameters[0].ParameterName;
				groupBySource = lambdaExpression.ExpressionBody;
			}

			captureSelectNewFieldNamesVisitor.FieldNames.Clear();// reduce override the map fields
			reduceDefiniton.Initializer.AcceptVisitor(captureSelectNewFieldNamesVisitor, null);
			reduceDefiniton.Initializer.AcceptChildren(captureQueryParameterNamesVisitorForReduce, null);

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

	    private VariableDeclaration TransformMapDefinition(out string entityName)
		{
			if (indexDefinition.Map.Trim().StartsWith("from"))
				return TransformMapDefinitionFromLinqQuerySyntax(out entityName);
			return TransformMapDefinitionFromLinqMethodSyntax(out entityName);
		}

		private VariableDeclaration TransformMapDefinitionFromLinqMethodSyntax(out string entityName)
		{
			var variableDeclaration = QueryParsingUtils.GetVariableDeclarationForLinqMethods(indexDefinition.Map);
			AddEntityNameFilteringIfNeeded(variableDeclaration, out entityName);

            variableDeclaration.AcceptVisitor(new AddDocumentIdToLambdas(), null);
			return variableDeclaration;
		}

        public class AddDocumentIdToLambdas : ICSharpCode.NRefactory.Visitors.AbstractAstTransformer
        {
            public override object VisitLambdaExpression(LambdaExpression lambdaExpression, object data)
            {
                AddDocumentIdFieldToLambdaIfCreatingNewObject(lambdaExpression);
                return base.VisitLambdaExpression(lambdaExpression, data);
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
					new List<Expression> { new PrimitiveExpression("Raven-Entity-Name", "Raven-Entity-Name") }
					);
				var whereMethod = new InvocationExpression(new MemberReferenceExpression(mre.TargetObject, "Where"),
				                                           new List<Expression>
				                                           {
				                                           	new LambdaExpression
				                                           	{
				                                           		Parameters =
				                                           			{
				                                           				new ParameterDeclarationExpression(null, "__document")
				                                           			},
				                                           		ExpressionBody = new BinaryOperatorExpression(
				                                           			metadata,
				                                           			BinaryOperatorType.Equality,
				                                           			new PrimitiveExpression(mre.MemberName, mre.MemberName)
				                                           			)
				                                           	}
				                                           });

				invocationExpression.TargetObject = new MemberReferenceExpression(whereMethod, targetExpression.MemberName);

			}
		}

		private static void AddDocumentIdFieldToLambdaIfCreatingNewObject(LambdaExpression lambdaExpression)
		{
			if (lambdaExpression.ExpressionBody is ObjectCreateExpression == false)
				return;
			var objectInitializer = ((ObjectCreateExpression)lambdaExpression.ExpressionBody).ObjectInitializer;

			var identifierExpression = new IdentifierExpression(lambdaExpression.Parameters[0].ParameterName);

			if (objectInitializer.CreateExpressions.OfType<NamedArgumentExpression>().Any(x => x.Name == "__document_id"))
				return;

			objectInitializer.CreateExpressions.Add(
				new NamedArgumentExpression
				{
					Name = "__document_id",
					Expression = new MemberReferenceExpression(identifierExpression, "__document_id")
				});
		}

		private VariableDeclaration TransformMapDefinitionFromLinqQuerySyntax(out string entityName)
		{
			entityName = null;
			var variableDeclaration = QueryParsingUtils.GetVariableDeclarationForLinqQuery(indexDefinition.Map, RequiresSelectNewAnonymousType);
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
					new List<Expression> { new PrimitiveExpression("Raven-Entity-Name", "Raven-Entity-Name") }
					);
				queryExpression.MiddleClauses.Insert(0, 
				                                     new QueryExpressionWhereClause
				                                     {
				                                     	Condition = 
				                                     		new BinaryOperatorExpression(
				                                     		metadata,
				                                     		BinaryOperatorType.Equality,
				                                     		new PrimitiveExpression(mre.MemberName, mre.MemberName)
				                                     		)
				                                     });
			}
			var selectOrGroupClause = queryExpression.SelectOrGroupClause;
			var projection = ((QueryExpressionSelectClause) selectOrGroupClause).Projection;
            if(projection is ObjectCreateExpression == false)
                return variableDeclaration;

			var objectInitializer = ((ObjectCreateExpression) projection).ObjectInitializer;

			var identifierExpression = new IdentifierExpression(queryExpression.FromClause.Identifier);

			if (objectInitializer.CreateExpressions.OfType<NamedArgumentExpression>().Any(x => x.Name == "__document_id"))
				return variableDeclaration;

			objectInitializer.CreateExpressions.Add(
				new NamedArgumentExpression
				{
					Name = "__document_id",
					Expression = new MemberReferenceExpression(identifierExpression, "__document_id")
				});
			return variableDeclaration;
		}

		public AbstractViewGenerator GenerateInstance()
		{
			TransformQueryToClass();
			
			GeneratedType = QueryParsingUtils.Compile(CompiledQueryText, CSharpSafeName, CompiledQueryText, extensions, basePath);
			
			return (AbstractViewGenerator) Activator.CreateInstance(GeneratedType);
		}
	}
}
