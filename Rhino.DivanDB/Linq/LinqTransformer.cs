using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Collections.Specialized;
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
        public string Name { get; private set; }
        private readonly string source;
        private readonly string rootQueryName;
        private readonly string path;
        private readonly Type rootQueryType;
        private CompilerResults compilerResults;


        public string PathToAssembly { get; private set; }

        public string Source
        {
            get { return source; }
        }

        public LinqTransformer(string name, string source, string rootQueryName, string path, Type rootQueryType)
        {
            Name = name;
            this.source = source;
            this.rootQueryName = rootQueryName;
            this.path = path;
            this.rootQueryType = rootQueryType;
        }

        public Type CompiledType
        {
            get
            {
                if (compilerResults == null)
                    Compile();
                PathToAssembly = compilerResults.PathToAssembly;
                return compilerResults.CompiledAssembly.GetType(Name);
            }
        }

        public string ImplicitClassSource { get; set; }

        public void Compile()
        {
            ImplicitClassSource = LinqQueryToImplicitClass();
            var outputPath = Path.Combine(path, Name + ".view.cs");
            File.WriteAllText(outputPath, ImplicitClassSource);
            var provider = new CSharpCodeProvider(new Dictionary<string, string> {{"CompilerVersion", "v3.5"}});
            var results = provider.CompileAssemblyFromFile(new CompilerParameters
            {
                GenerateExecutable = false,
                GenerateInMemory = false,
                IncludeDebugInformation = true,
                ReferencedAssemblies =
                                                               {
                                                                   typeof (AbstractViewGenerator).Assembly.Location,
                                                                   typeof (NameValueCollection).Assembly.Location,
                                                                   typeof (object).Assembly.Location,
                                                                   typeof (System.Linq.Enumerable).Assembly.Location,
                                                                   rootQueryType.Assembly.Location
                                                               },
            }, outputPath);
            if (results.Errors.HasErrors)
            {
                var sb = new StringBuilder().AppendLine();
                foreach (CompilerError error in results.Errors)
                {
                    sb.AppendLine(error.ToString());
                }
                throw new InvalidOperationException(sb.ToString());
            }
            compilerResults = results;
        }

        public string LinqQueryToImplicitClass()
        {
            var preProcessedSource = "var query = " + source;
            var parser = ParserFactory.CreateParser(SupportedLanguage.CSharp, new StringReader(preProcessedSource));
            var block = parser.ParseBlock();

            VariableDeclaration variable = GetVariableDeclaration(block);

            var visitor = new TransformVisitor{Name = Name};
            block.AcceptVisitor(visitor, null);

            ((QueryExpression) variable.Initializer).SelectOrGroupClause.AcceptVisitor(
                new TurnOutwardReferenceToString {Identifier = visitor.Identifier}, null);

            var type = new TypeDeclaration(Modifiers.Public, new List<AttributeSection>())
            {
                BaseTypes =
                    {
                        new TypeReference("AbstractViewGenerator")
                    },
                Name = Name,
                Type = ClassType.Class
            };

            var ctor = new ConstructorDeclaration(Name,
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
                                       new MemberReferenceExpression(new ThisReferenceExpression(), "IndexDefinition"),
                                       AssignmentOperatorType.Assign,
                                       new LambdaExpression
                                       {
                                           Parameters =
                                               {
                                                   new ParameterDeclarationExpression(new TypeReference("System.Collections.Generic.IEnumerable<"+rootQueryType+">"), rootQueryName)
                                               },
                                           ExpressionBody = variable.Initializer
                                       })));
            foreach (var fieldName in visitor.FieldNames)
            {
                ctor.Body.AddChild(new ExpressionStatement(
                    new InvocationExpression(
                        new MemberReferenceExpression(
                            new IdentifierExpression("AccessedFields"), "Add"),
                        new List<Expression> { new PrimitiveExpression(fieldName, fieldName) }
                        )
                    ));
            }
            
            var unit = new CompilationUnit();
            unit.AddChild(new Using(typeof(AbstractViewGenerator).Namespace));
            unit.AddChild(new Using(typeof(System.Linq.Enumerable).Namespace));
            unit.AddChild(new Using(typeof(int).Namespace));
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