using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Text;
using ICSharpCode.NRefactory;
using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.PrettyPrinter;
using Microsoft.CSharp;

namespace Rhino.DivanDB.Linq
{
    public class LinqTransformer
    {
        private readonly string source;
        private readonly string rootQueryName;
        private readonly Type rootQueryType;
        private string name;

        public LinqTransformer(string source, string rootQueryName, Type rootQueryType)
        {
            this.source = source;
            this.rootQueryName = rootQueryName;
            this.rootQueryType = rootQueryType;
        }

        public Type Compile()
        {
            var implicitClassSource = LinqQueryToImplicitClass();
            var tempFileName = Path.GetTempFileName()+".cs";
            File.WriteAllText(tempFileName, implicitClassSource);
            Debug.WriteLine(tempFileName);
            var provider = new CSharpCodeProvider(new Dictionary<string, string> { { "CompilerVersion", "v3.5" } });
            var results = provider.CompileAssemblyFromFile(new CompilerParameters
            {
                GenerateExecutable = false,
                GenerateInMemory = false,
                IncludeDebugInformation = true,
                ReferencedAssemblies =
                    {
                        typeof(AbstractViewGenerator<>).Assembly.Location,
                        typeof(NameValueCollection).Assembly.Location,
                        typeof(object).Assembly.Location,
                        typeof(System.Linq.Enumerable).Assembly.Location,
                        rootQueryType.Assembly.Location
                    },
            }, tempFileName);
            if (results.Errors.HasErrors)
            {
                var sb = new StringBuilder().AppendLine();
                foreach (CompilerError error in results.Errors)
                {
                    sb.AppendLine(error.ToString());
                }
                throw new InvalidOperationException(sb.ToString());
            }

            return results.CompiledAssembly.GetType(name);
        }

        private string LinqQueryToImplicitClass()
        {
            var parser = ParserFactory.CreateParser(SupportedLanguage.CSharp, new StringReader(source));
            var block = parser.ParseBlock();

            var visitor = new TransformVisitor();
            block.AcceptVisitor(visitor, null);

            VariableDeclaration variable = GetVariableDeclaration(block);

            name = variable.Name;

            var type = new TypeDeclaration(Modifiers.Public, new List<AttributeSection>())
            {
                BaseTypes =
                    {
                        new TypeReference("AbstractViewGenerator<"+rootQueryType+">")
                    },
                Name = name,
                Type = ClassType.Class
            };

            var ctor = new ConstructorDeclaration(name,
                                                  Modifiers.Public,
                                                  new List<ParameterDeclarationExpression>(), null);
            type.Children.Add(ctor);
            ctor.Body = new BlockStatement();
            ctor.Body.AddChild(new ExpressionStatement(
                                   new AssignmentExpression(
                                       new MemberReferenceExpression(new ThisReferenceExpression(), "ViewText"),
                                       AssignmentOperatorType.Assign,
                                       new PrimitiveExpression(source, source))));
            ctor.Body.AddChild(new ExpressionStatement(
                                   new AssignmentExpression(
                                       new MemberReferenceExpression(new ThisReferenceExpression(), "ViewDefinition"),
                                       AssignmentOperatorType.Assign,
                                       new LambdaExpression
                                       {
                                           Parameters =
                                               {
                                                   new ParameterDeclarationExpression(new TypeReference("System.Collections.Generic.IEnumerable<"+rootQueryType+">"), rootQueryName)
                                               },
                                           ExpressionBody = variable.Initializer
                                       })));

            
            var unit = new CompilationUnit();
            unit.AddChild(new Using(typeof(AbstractViewGenerator<>).Namespace));
            unit.AddChild(new Using(typeof(System.Linq.Enumerable).Namespace));
            unit.AddChild(type);

            var output = new CSharpOutputVisitor();
            unit.AcceptVisitor(output, null);

            return output.Text;
        }

        private static VariableDeclaration GetVariableDeclaration(INode block)
        {
            if (block.Children.Count != 1)
                throw new InvalidOperationException("Only one statement is allowed");

            var declaration = block.Children[0] as LocalVariableDeclaration;
            if (declaration == null)
                throw new InvalidOperationException("Only local variable decleration are allowed");

            if (declaration.Variables.Count != 1)
                throw new InvalidOperationException("Only one variable declaration is allowed");

            var variable = declaration.Variables[0];

            if (variable.Initializer == null)
                throw new InvalidOperationException("Variable declaration must have an initializer");
            return variable;
        }
    }
}