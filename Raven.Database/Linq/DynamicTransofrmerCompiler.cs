using System;
using System.Text;
using System.Text.RegularExpressions;
using ICSharpCode.NRefactory.CSharp;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Linq.Ast;
using Raven.Database.Plugins;

namespace Raven.Database.Linq
{
	/// <summary>
	/// 	Takes a query expression as string, and compile them.
	/// 	Along the way we apply some minimal transformations, the end result is an instance
	/// 	of AbstractTransformer, representing the map/reduce functions
	/// </summary>
	public class DynamicTransformerCompiler : DynamicCompilerBase
	{
		private readonly TransformerDefinition transformerDefinition;

		public DynamicTransformerCompiler(
			TransformerDefinition transformerDefinition,
			InMemoryRavenConfiguration configuration, OrderedPartCollection<AbstractDynamicCompilationExtension> extensions, string name, string basePath) : base(configuration, extensions, name, basePath)
		{
			this.transformerDefinition = transformerDefinition;
		}

		public AbstractTransformer GenerateInstance()
		{
			TransformToClass();
			GeneratedType = QueryParsingUtils.Compile(CompiledQueryText, CSharpSafeName, CompiledQueryText, extensions, basePath, configuration);

			var abstractTransformer = (AbstractTransformer)Activator.CreateInstance(GeneratedType);
			abstractTransformer.SourceCode = CompiledQueryText;
			abstractTransformer.Init(transformerDefinition);
			return abstractTransformer;
		}

		private void TransformToClass()
		{
			if (transformerDefinition.TransformResults == null)
				throw new TransformCompilationException("Cannot compile a transformer without a transformer function");

		    try
		    {
                CSharpSafeName = "Transformer_" + Regex.Replace(Name, @"[^\w\d]", "_");  
		        var type = new TypeDeclaration
		        {
		            Modifiers = Modifiers.Public,
		            BaseTypes =
		            {
		                new SimpleType(typeof (AbstractTransformer).FullName)
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

		        VariableInitializer translatorDeclaration;

		        if (transformerDefinition.TransformResults.Trim().StartsWith("from"))
		        {
		            translatorDeclaration =
		                QueryParsingUtils.GetVariableDeclarationForLinqQuery(transformerDefinition.TransformResults,
		                                                                     requiresSelectNewAnonymousType: false);
		        }
		        else
		        {
		            translatorDeclaration =
		                QueryParsingUtils.GetVariableDeclarationForLinqMethods(transformerDefinition.TransformResults,
		                                                                       requiresSelectNewAnonymousType: false);
		        }

		        translatorDeclaration.AcceptVisitor(new ThrowOnInvalidMethodCallsForTransformResults(), null);


		        // this.Translator = (results) => from doc in results ...;
		        ctor.Body.Statements.Add(new ExpressionStatement(
		                                     new AssignmentExpression(
		                                         new MemberReferenceExpression(new ThisReferenceExpression(),
		                                                                       "TransformResultsDefinition"),
		                                         AssignmentOperatorType.Assign,
		                                         new LambdaExpression
		                                         {
		                                             Parameters =
		                                             {
		                                                 new ParameterDeclaration(null, "results")
		                                             },
		                                             Body = translatorDeclaration.Initializer.Clone()
		                                         })));


		        CompiledQueryText = QueryParsingUtils.GenerateText(type, extensions);
		        var sb = new StringBuilder("@\"");
		        sb.AppendLine(transformerDefinition.TransformResults.Replace("\"", "\"\""));
		        sb.Append("\"");
		        CompiledQueryText = CompiledQueryText.Replace('"' + uniqueTextToken + '"', sb.ToString());
		    }
		    catch (Exception ex)
		    {
		        throw new TransformCompilationException(ex.Message, ex);
		    }
		}
	}
}