//-----------------------------------------------------------------------
// <copyright file="DynamicViewCompiler"" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using ICSharpCode.NRefactory.CSharp;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using System.Linq;
using Raven.Database.Linq.Ast;
using Raven.Database.Plugins;
using Raven.Database.Util;

namespace Raven.Database.Linq
{
	/// <summary>
	/// 	Takes a set of query expressions as strings, and compile them.
	/// 	Along the way we apply some minimal transformations, the end result is an instance
	/// 	of AbstractViewGenerator, representing the map/reduce functions
	/// </summary>
	internal class DynamicViewCompiler : DynamicCompilerBase
	{
		private readonly IndexDefinition indexDefinition;

		private readonly HashSet<string> _fieldNames = new HashSet<string>();
		private readonly Dictionary<string, Expression> _selectExpressions = new Dictionary<string, Expression>();
		private readonly CaptureQueryParameterNamesVisitor captureQueryParameterNamesVisitorForMap = new CaptureQueryParameterNamesVisitor();
		private readonly CaptureQueryParameterNamesVisitor captureQueryParameterNamesVisitorForReduce = new CaptureQueryParameterNamesVisitor();
		private readonly TransformFromClauses transformFromClauses = new TransformFromClauses();

		public DynamicViewCompiler(string name, IndexDefinition indexDefinition, string basePath)
			: this(name, indexDefinition, new OrderedPartCollection<AbstractDynamicCompilationExtension>(), basePath, new RavenConfiguration())
		{ }

		public DynamicViewCompiler(string name, IndexDefinition indexDefinition, OrderedPartCollection<AbstractDynamicCompilationExtension> extensions, string basePath, InMemoryRavenConfiguration configuration)
			:base(configuration, extensions, name, basePath)
		{
			this.indexDefinition = indexDefinition;
			RequiresSelectNewAnonymousType = true;
		}

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
					new StringLiteralExpression(uniqueTextToken)));
			body.Statements.Add(viewText);

			var ctor = new ConstructorDeclaration
			{
				Name = CSharpSafeName,
				Modifiers = Modifiers.Public,
				Body = body
			};
			type.Members.Add(ctor);

			HandleMapFunctions(ctor);

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

			sb.Length = sb.Length - 2;

			sb.Append("\"");
			CompiledQueryText = CompiledQueryText.Replace('"' + uniqueTextToken + '"', sb.ToString());
		}

	    private void HandleMapFunctions(ConstructorDeclaration ctor)
	    {
	        foreach (var map in indexDefinition.Maps)
	        {
	            try
	            {
	                HandleMapFunction(ctor, map);
	            }
	            catch (InvalidOperationException ex)
	            {
	                throw new IndexCompilationException(ex.Message, ex)
	                {
	                    IndexDefinitionProperty = "Maps",
	                    ProblematicText = map
	                };
	            }
	        }
	    }

	    private bool firstMap = true;
		private void HandleMapFunction(ConstructorDeclaration ctor, string map)
		{
			string entityName;

			bool querySyntax = map.Trim().StartsWith("from");

			var captureSelectNewFieldNamesVisitor = new CaptureSelectNewFieldNamesVisitor(querySyntax == false, _fieldNames, _selectExpressions);

			VariableInitializer mapDefinition = querySyntax ?
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
				var secondMapFieldNames = new CaptureSelectNewFieldNamesVisitor(querySyntax == false, new HashSet<string>(), new Dictionary<string, Expression>());
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


		private void HandleReduceDefinition(ConstructorDeclaration ctor)
		{
		    try
		    {

		        if (!indexDefinition.IsMapReduce)
		            return;
		        VariableInitializer reduceDefinition;
		        AstNode groupBySource;
		        string groupByParameter;
		        string groupByIdentifier;
			    bool querySyntax = indexDefinition.Reduce.Trim().StartsWith("from");
			    if (querySyntax)
		        {
		            reduceDefinition = QueryParsingUtils.GetVariableDeclarationForLinqQuery(indexDefinition.Reduce,
		                                                                                    RequiresSelectNewAnonymousType);
		            var queryExpression = ((QueryExpression) reduceDefinition.Initializer);
		            var queryContinuationClause = GetQueryContinuationClauseForGroupBy(queryExpression);
                    if (queryContinuationClause == null)
                    {
                        throw new IndexCompilationException("Reduce query must contain a 'group ... into ...' clause")
                        {
                            ProblematicText = indexDefinition.Reduce,
                            IndexDefinitionProperty = "Reduce",
                        };
                    }

		            var queryGroupClause = queryContinuationClause.PrecedingQuery.Clauses.OfType<QueryGroupClause>().FirstOrDefault();
                    if (queryGroupClause == null)
                    {
                        throw new IndexCompilationException("Reduce query must contain a 'group ... into ...' clause")
                        {
                            ProblematicText = indexDefinition.Reduce,
                            IndexDefinitionProperty = "Reduce",
                        };
                    }

		            groupByIdentifier = queryContinuationClause.Identifier;
		            groupBySource = queryGroupClause.Key;
		            groupByParameter =
		                queryContinuationClause.PrecedingQuery.Clauses.OfType<QueryFromClause>().First().Identifier;
		        }
		        else
		        {
		            reduceDefinition = QueryParsingUtils.GetVariableDeclarationForLinqMethods(indexDefinition.Reduce,
		                                                                                      RequiresSelectNewAnonymousType);
		            var initialInvocation = ((InvocationExpression) reduceDefinition.Initializer);
		            var invocation = initialInvocation;
		            var target = (MemberReferenceExpression) invocation.Target;
		            while (target.MemberName != "GroupBy")
		            {
                        if (!(target.Target is InvocationExpression))
                        {
                            // we've reached the initial results variable without encountering a GroupBy call
                            throw new IndexCompilationException("Reduce expression must contain a call to GroupBy")
                            {
                                ProblematicText = indexDefinition.Reduce,
                                IndexDefinitionProperty = "Reduce",
                            };
                        }

		                invocation = (InvocationExpression) target.Target;
		                target = (MemberReferenceExpression) invocation.Target;
		            }
		            var lambdaExpression = GetLambdaExpression(invocation);
		            groupByParameter = lambdaExpression.Parameters.First().Name;
		            groupBySource = lambdaExpression.Body;
					groupByIdentifier = groupByParameter;
		        }

		        var mapFields = _fieldNames.ToList();
				_fieldNames.Clear(); // reduce override the map fields
				_selectExpressions.Clear();
				reduceDefinition.Initializer.AcceptVisitor(new CaptureSelectNewFieldNamesVisitor(querySyntax == false, _fieldNames, _selectExpressions), null);
		        reduceDefinition.Initializer.AcceptVisitor(captureQueryParameterNamesVisitorForReduce, null);
				reduceDefinition.Initializer.AcceptVisitor(new ThrowOnInvalidMethodCallsInReduce(groupByIdentifier), null);

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
		                                             Body = reduceDefinition.Initializer.Clone()
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
		                                                 new ParameterDeclaration(null, groupByParameter)
		                                             },
		                                             Body = groupBySource.Clone()
		                                         })));
		    }
		    catch (InvalidOperationException ex)
		    {
		        throw new IndexCompilationException(ex.Message,ex)
		        {
		            ProblematicText = indexDefinition.Reduce,
                    IndexDefinitionProperty = "Reduce",
		        };
		    }
		}

		private static QueryContinuationClause GetQueryContinuationClauseForGroupBy(QueryExpression queryExpression)
		{
			return queryExpression.Descendants.OfType<QueryContinuationClause>().FirstOrDefault(q => q.PrecedingQuery.Clauses.Any(x=>x is QueryGroupClause));
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
			mapFields.Remove(Constants.DocumentIdFieldName);
			var reduceFields = _fieldNames;
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
			AddInformation(ctor, _fieldNames, "AddField");
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
				if (initializers.OfType<NamedExpression>().Any(x => x.Name == Constants.DocumentIdFieldName))
					return false;

				var parameter = lambdaExpression.Parameters.First();
				var identifier = parameter.Name;

				// Support getting the __document_id from IGrouping parameter
				var castExpression = parameter.Parent.Parent.Parent as CastExpression;
				if (castExpression != null)
				{
					var simpleType = castExpression.Type as SimpleType;
					if (simpleType != null && simpleType.Identifier == "Func<IGrouping<dynamic,dynamic>, dynamic>")
					{
						identifier += ".Key";
					}
				}

				var identifierExpression = new IdentifierExpression(identifier);
				objectCreateExpression.Initializers.Add(new NamedExpression
				{
					Name = Constants.DocumentIdFieldName,
					Expression = new MemberReferenceExpression(identifierExpression, Constants.DocumentIdFieldName)
				});

				return true;
			}
		}

		public class AddDocumentIdToQueries : DepthFirstAstVisitor<object, object>
		{
			public override object VisitAnonymousTypeCreateExpression(AnonymousTypeCreateExpression objectCreateExpression, object data)
			{

				var initializers = objectCreateExpression.Initializers;
				if (initializers.OfType<NamedExpression>().Any(x => x.Name == Constants.DocumentIdFieldName))
					return false;

				// need to find the current from / group identifier

				var parentQueryClause = GetRelevantClause(objectCreateExpression);
				
				var queryFromClause = parentQueryClause as QueryFromClause;
				if (queryFromClause != null)
				{
					string identifier = queryFromClause.Identifier;
					var prev = queryFromClause.GetPrevNode();
					while (prev != null)
					{
						var fromClause = prev as QueryFromClause;
						if (fromClause != null)
						{
							identifier = fromClause.Identifier;
							break;
						}
						var continuationClause = prev as QueryContinuationClause;
						if (continuationClause != null)
						{
							identifier = continuationClause.Identifier;
							break;
						}
						prev = prev.GetPrevNode();
					}

					objectCreateExpression.Initializers.Add(new NamedExpression
					{
						Name = Constants.DocumentIdFieldName,
						Expression = new MemberReferenceExpression(new IdentifierExpression(identifier), Constants.DocumentIdFieldName)
					});
				}
				var queryGroupClause = parentQueryClause as QueryGroupClause;
				if (queryGroupClause != null)
				{
					var projection = queryGroupClause.Projection;
					var mre = projection as MemberReferenceExpression;
					while (mre != null)
					{
						projection = mre.Target;
						mre = projection as MemberReferenceExpression;
					}
					objectCreateExpression.Initializers.Add(new NamedExpression
					{
						Name = Constants.DocumentIdFieldName,
						Expression = new MemberReferenceExpression(projection.Clone(), Constants.DocumentIdFieldName)
					});
				}
				var queryContinuationClause = parentQueryClause as QueryContinuationClause;
				if (queryContinuationClause != null)
				{
					Expression identifier = new IdentifierExpression(queryContinuationClause.Identifier);
					if(queryContinuationClause.PrecedingQuery.Clauses.LastOrNullObject() is QueryGroupClause)
						identifier = new MemberReferenceExpression(identifier, "Key");

					objectCreateExpression.Initializers.Add(new NamedExpression
					{
						Name = Constants.DocumentIdFieldName,
						Expression = new MemberReferenceExpression(identifier, Constants.DocumentIdFieldName)
					});
				}

				return data;
			}

			private static QueryClause GetRelevantClause(AstNode node)
			{
				var relevantClause = node.GetParent<QueryClause>();
				do
				{
					if (relevantClause == null)
						throw new IOException("Can't figure out how to find the relevant clause for this query");

					if (relevantClause is QueryGroupClause)
						return relevantClause;
					var queryFromClause = relevantClause as QueryFromClause;
					if (queryFromClause != null)
					{
						var parentClause = GetPreviousQueryFromClause(queryFromClause);
						
						if (parentClause != null)
						{
							relevantClause = parentClause;
							continue;
						}
						return relevantClause;
					}
					if (relevantClause is QueryContinuationClause)
						return relevantClause;
					relevantClause = relevantClause.GetPrevNode() as QueryClause;
				} while (true);
			}

			private static QueryFromClause GetPreviousQueryFromClause(AstNode node)
			{
				while (true)
				{
					node = node.GetPrevNode();
					if (node == null)
						return null;
					var qfc = node as QueryFromClause;
					if (qfc != null)
						return qfc;
				}
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

				// string.Equals(doc["@metadata"]["Raven-Entity-Name"], "Blogs", StringComparison.OrdinalIgnoreCase)
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
			var fromClause = GetFromClause(queryExpression);
			var expression = fromClause.Expression;
			HandleCollectionName(expression, fromClause, queryExpression, ref entityName);
			var projection = 
				QueryParsingUtils.GetAnonymousCreateExpression(
					queryExpression.Clauses.OfType<QuerySelectClause>().First().Expression);

			var anonymousTypeCreateExpression = projection as AnonymousTypeCreateExpression;

			if (anonymousTypeCreateExpression == null)
				return variableDeclaration;

			var objectInitializer = anonymousTypeCreateExpression.Initializers;

			var identifierExpression = new IdentifierExpression(fromClause.Identifier);

			if (objectInitializer.OfType<NamedExpression>().Any(x => x.Name == Constants.DocumentIdFieldName))
				return variableDeclaration;

			variableDeclaration.AcceptVisitor(new AddDocumentIdToQueries(), null);

			variableDeclaration.AcceptVisitor(this.transformFromClauses, null);

			return variableDeclaration;
		}

		private static QueryFromClause GetFromClause(QueryExpression queryExpression)
		{
			var first = queryExpression.Clauses.First();
			var queryFromClause = first as QueryFromClause;
			if (queryFromClause != null)
				return queryFromClause;
			var queryContinuationClause = first as QueryContinuationClause;
			if (queryContinuationClause == null)
				throw new InvalidOperationException("Don't understand how to parse this query");
			return GetFromClause(queryContinuationClause.PrecedingQuery);
		}

		private static void HandleCollectionName(Expression expression, QueryFromClause fromClause, QueryExpression queryExpression, ref string entityName)
		{
			// from d in docs.Users.SelectMany(x=>x.Roles) ...
			// from d in docs.Users.Where(x=>x.IsActive)   ...
			var mie = expression as InvocationExpression;
			string methodToCall = null;
			if (mie != null)
			{
				expression = mie.Target;

				var target = expression as MemberReferenceExpression;
				if (target != null)
				{
					methodToCall = target.MemberName;
					expression = target.Target;
				}
			}

			var mre = expression as MemberReferenceExpression;
			if (mre == null) 
				return;

			string oldIdentifier = fromClause.Identifier;
			if (mie != null)
			{
				fromClause.Identifier += "Item";
			}

			entityName = mre.MemberName;
			fromClause.Expression = mre.Target;
			//doc["@metadata"]["Raven-Entity-Name"]
			var metadata = new IndexerExpression(
				new IndexerExpression(new IdentifierExpression(fromClause.Identifier), new List<Expression> {new StringLiteralExpression("@metadata")}),
				new List<Expression> {new StringLiteralExpression(Constants.RavenEntityName)}
				);

			// string.Equals(doc["@metadata"]["Raven-Entity-Name"], "Blogs", StringComparison.OrdinalIgnoreCase)
			var binaryOperatorExpression =
				new InvocationExpression(
					new MemberReferenceExpression(new TypeReferenceExpression(new PrimitiveType("string")), "Equals"),
					new List<Expression>
					{
						metadata,
						new StringLiteralExpression(mre.MemberName),
						new MemberReferenceExpression(new TypeReferenceExpression(new SimpleType(typeof (StringComparison).FullName)), "InvariantCultureIgnoreCase")
					});

			var queryWhereClause = new QueryWhereClause
			{
				Condition = binaryOperatorExpression
			};
			((QueryExpression)fromClause.Parent).Clauses.InsertAfter(fromClause,
				queryWhereClause);

			if (mie != null)
			{
				var newSource = new ArrayCreateExpression
				{
					Initializer = new ArrayInitializerExpression(new IdentifierExpression(fromClause.Identifier)),
					AdditionalArraySpecifiers = { new ArraySpecifier(1) }
				};
				queryExpression.Clauses.InsertAfter(queryWhereClause, new QueryFromClause
				{
					Identifier = oldIdentifier,
					Expression = new InvocationExpression(new MemberReferenceExpression(newSource, methodToCall), mie.Arguments.Select(x => x.Clone()))
				});
			}
		}

		public AbstractViewGenerator GenerateInstance()
		{
			TransformQueryToClass();

			GeneratedType = QueryParsingUtils.Compile(CompiledQueryText, CSharpSafeName, CompiledQueryText, extensions, basePath, configuration);

			var abstractViewGenerator = (AbstractViewGenerator)Activator.CreateInstance(GeneratedType);
			abstractViewGenerator.SourceCode = CompiledQueryText;
			abstractViewGenerator.Init(indexDefinition);
			return abstractViewGenerator;
		}
	}
}
