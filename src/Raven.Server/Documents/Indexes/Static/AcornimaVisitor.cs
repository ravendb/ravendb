using System;
using System.Collections.Generic;
using System.Linq;
using Acornima.Ast;

namespace Raven.Server.Documents.Indexes.Static
{
    public class AcornimaVisitor
    {
        public virtual void VisitProgram(Acornima.Ast.Program program)
        {
            foreach (var statement in program.Body)
            {
                VisitStatement(statement);
            }
        }

        public virtual void VisitStatement(Statement statement)
        {
            if (statement == null)
                return;

            switch (statement.Type)
            {
                case NodeType.BlockStatement:
                    VisitBlockStatement(statement.As<BlockStatement>());
                    break;
                case NodeType.BreakStatement:
                    VisitBreakStatement(statement.As<BreakStatement>());
                    break;
                case NodeType.ContinueStatement:
                    VisitContinueStatement(statement.As<ContinueStatement>());
                    break;
                case NodeType.DoWhileStatement:
                    VisitDoWhileStatement(statement.As<DoWhileStatement>());
                    break;
                case NodeType.DebuggerStatement:
                    VisitDebuggerStatement(statement.As<DebuggerStatement>());
                    break;
                case NodeType.EmptyStatement:
                    VisitEmptyStatement(statement.As<EmptyStatement>());
                    break;
                case NodeType.ExpressionStatement:
                    VisitExpressionStatement(statement.As<ExpressionStatement>());
                    break;
                case NodeType.ForStatement:
                    VisitForStatement(statement.As<ForStatement>());
                    break;
                case NodeType.ForInStatement:
                    VisitForInStatement(statement.As<ForInStatement>());
                    break;
                case NodeType.FunctionDeclaration:
                    VisitFunctionDeclaration(statement.As<FunctionDeclaration>());
                    break;
                case NodeType.IfStatement:
                    VisitIfStatement(statement.As<IfStatement>());
                    break;
                case NodeType.LabeledStatement:
                    VisitLabeledStatement(statement.As<LabeledStatement>());
                    break;
                case NodeType.ReturnStatement:
                    VisitReturnStatement(statement.As<ReturnStatement>());
                    break;
                case NodeType.SwitchStatement:
                    VisitSwitchStatement(statement.As<SwitchStatement>());
                    break;
                case NodeType.ThrowStatement:
                    VisitThrowStatement(statement.As<ThrowStatement>());
                    break;
                case NodeType.TryStatement:
                    VisitTryStatement(statement.As<TryStatement>());
                    break;
                case NodeType.VariableDeclaration:
                    VisitVariableDeclaration(statement.As<VariableDeclaration>());
                    break;
                case NodeType.WhileStatement:
                    VisitWhileStatement(statement.As<WhileStatement>());
                    break;
                case NodeType.WithStatement:
                    VisitWithStatement(statement.As<WithStatement>());
                    break;
                case NodeType.Program:
                    VisitProgram(statement.As<Acornima.Ast.Program>());
                    break;
                case NodeType.CatchClause:
                    VisitCatchClause(statement.As<CatchClause>());
                    break;
                default:
                    VisitUnknownNode(statement);
                    break;
            }
        }

        public virtual void VisitUnknownNode(Node node)
        {
            throw new NotImplementedException($"Acornima visitor doesn't support nodes of type {node.Type}, you can override VisitUnknownNode to handle this case.");
        }

        public virtual void VisitUnknownObject(object obj)
        {
            throw new NotImplementedException($"Acornima visitor doesn't support object of type {obj?.GetType()}, you can override VisitUnknownObject to handle this case.");
        }

        private void VisitCatchClause(CatchClause catchClause)
        {
            VisitIdentifier(catchClause.Param.As<Identifier>());
            VisitStatement(catchClause.Body);
        }

        public virtual void VisitFunctionDeclaration(FunctionDeclaration functionDeclaration)
        {
            foreach (var p in functionDeclaration.Params)
            {
                Visit(p);
            }

            VisitBlockStatement(functionDeclaration.Body);
        }

        public virtual void VisitWithStatement(WithStatement withStatement)
        {
            VisitExpression(withStatement.Object);
            VisitStatement(withStatement.Body);
        }

        public virtual void VisitWhileStatement(WhileStatement whileStatement)
        {
            VisitExpression(whileStatement.Test);
            VisitStatement(whileStatement.Body);
        }

        public virtual void VisitVariableDeclaration(VariableDeclaration variableDeclaration)
        {
            foreach (var declaration in variableDeclaration.Declarations)
            {
                VisitIdentifier(declaration.Id.As<Identifier>());
                if (declaration.Init != null)
                {
                    VisitExpression(declaration.Init);
                }
            }
        }

        public virtual void VisitTryStatement(TryStatement tryStatement)
        {
            VisitStatement(tryStatement.Block);
            if (tryStatement.Handler != null)
            {
                VisitCatchClause(tryStatement.Handler);
            }

            if (tryStatement.Finalizer != null)
            {
                VisitStatement(tryStatement.Finalizer);
            }


        }

        public virtual void VisitThrowStatement(ThrowStatement throwStatement)
        {
            VisitExpression(throwStatement.Argument);
        }

        public virtual void VisitSwitchStatement(SwitchStatement switchStatement)
        {
            VisitExpression(switchStatement.Discriminant);
            foreach (var c in switchStatement.Cases)
            {
                VisitSwitchCase(c);
            }
        }

        public virtual void VisitSwitchCase(SwitchCase switchCase)
        {
            if (switchCase.Test != null)
                VisitExpression(switchCase.Test);

            foreach (var s in switchCase.Consequent)
            {
                //In most cases it is going to be statment
                if (s is Statement statment)
                {
                    VisitStatement(statment);
                }
                else if (s is Node node)
                {
                    Visit(node);
                }
                else
                {
                    VisitUnknownObject(s);
                }
            }
        }

        public virtual void VisitReturnStatement(ReturnStatement returnStatement)
        {
            if (returnStatement.Argument == null)
                return;
            VisitExpression(returnStatement.Argument);
        }

        public virtual void VisitLabeledStatement(LabeledStatement labeledStatement)
        {
            VisitStatement(labeledStatement.Body);
        }

        public virtual void VisitIfStatement(IfStatement ifStatement)
        {
            VisitExpression(ifStatement.Test);
            VisitStatement(ifStatement.Consequent);
            VisitStatement(ifStatement.Alternate);
        }

        public virtual void VisitEmptyStatement(EmptyStatement emptyStatement)
        {
        }

        public virtual void VisitDebuggerStatement(DebuggerStatement debuggerStatement)
        {
        }

        public virtual void VisitExpressionStatement(ExpressionStatement expressionStatement)
        {
            VisitExpression(expressionStatement.Expression);
        }

        public virtual void VisitForStatement(ForStatement forStatement)
        {
            if (forStatement.Init != null)
            {
                if (forStatement.Init.Type == NodeType.VariableDeclaration)
                {
                    VisitStatement(forStatement.Init.As<Statement>());
                }
                else
                {
                    VisitExpression(forStatement.Init.As<Expression>());
                }
            }
            VisitExpression(forStatement.Test);
            VisitStatement(forStatement.Body);
            if (forStatement.Update != null)
            {
                VisitExpression(forStatement.Update);
            }
        }

        public virtual void VisitForInStatement(ForInStatement forInStatement)
        {
            Identifier identifier = forInStatement.Left.Type == NodeType.VariableDeclaration
                ? forInStatement.Left.As<VariableDeclaration>().Declarations.First().Id.As<Identifier>()
                : forInStatement.Left.As<Identifier>();
            VisitExpression(identifier);
            VisitExpression(forInStatement.Right);
            VisitStatement(forInStatement.Body);
        }

        public virtual void VisitDoWhileStatement(DoWhileStatement doWhileStatement)
        {
            VisitStatement(doWhileStatement.Body.As<Statement>());
            VisitExpression(doWhileStatement.Test);
        }

        public virtual void VisitExpression(Expression expression)
        {
            if (expression == null)
                return;
            switch (expression.Type)
            {
                case NodeType.AssignmentExpression:
                    VisitAssignmentExpression(expression.As<AssignmentExpression>());
                    break;
                case NodeType.ArrayExpression:
                    VisitArrayExpression(expression.As<ArrayExpression>());
                    break;
                case NodeType.BinaryExpression:
                    VisitBinaryExpression(expression.As<BinaryExpression>());
                    break;
                case NodeType.CallExpression:
                    VisitCallExpression(expression.As<CallExpression>());
                    break;
                case NodeType.ConditionalExpression:
                    VisitConditionalExpression(expression.As<ConditionalExpression>());
                    break;
                case NodeType.FunctionExpression:
                    VisitFunctionExpression(expression.As<FunctionExpression>());
                    break;
                case NodeType.Identifier:
                    VisitIdentifier(expression.As<Identifier>());
                    break;
                case NodeType.Literal:
                    VisitLiteral(expression.As<Literal>());
                    break;
                case NodeType.LogicalExpression:
                    VisitLogicalExpression(expression.As<BinaryExpression>());
                    break;
                case NodeType.MemberExpression:
                    VisitMemberExpression(expression.As<MemberExpression>());
                    break;
                case NodeType.NewExpression:
                    VisitNewExpression(expression.As<NewExpression>());
                    break;
                case NodeType.ObjectExpression:
                    VisitObjectExpression(expression.As<ObjectExpression>());
                    break;
                case NodeType.SequenceExpression:
                    VisitSequenceExpression(expression.As<SequenceExpression>());
                    break;
                case NodeType.ThisExpression:
                    VisitThisExpression(expression.As<ThisExpression>());
                    break;
                case NodeType.UpdateExpression:
                    VisitUpdateExpression(expression.As<UpdateExpression>());
                    break;
                case NodeType.UnaryExpression:
                    VisitUnaryExpression(expression.As<UnaryExpression>());
                    break;
                case NodeType.ArrowFunctionExpression:
                    VisitArrowFunctionExpression(expression.As<ArrowFunctionExpression>());
                    break;
                case NodeType.SpreadElement:
                    VisitSpreadElement(expression.As<SpreadElement>());
                    break;
                default:
                    VisitUnknownNode(expression);
                    break;
            }
        }

        public virtual void VisitArrowFunctionExpression(ArrowFunctionExpression arrowFunctionExpression)
        {
            //Here we construct the function so if we iterate only functions we will be able to iterate ArrowFunctions too
            var statement =
                arrowFunctionExpression.Expression
                    ? new FunctionBody(NodeList.From(new List<Statement> { new ReturnStatement(arrowFunctionExpression.Body.As<Expression>()) }), strict: true)
                    : arrowFunctionExpression.Body.As<FunctionBody>();
            var func = new FunctionExpression(
                new Identifier(null),
                arrowFunctionExpression.Params,
                statement,
                generator: false,
                async: false);
            
            VisitFunctionExpression(func);
        }

        public virtual void VisitUnaryExpression(UnaryExpression unaryExpression)
        {
            VisitExpression(unaryExpression.Argument);
        }

        public virtual void VisitUpdateExpression(UpdateExpression updateExpression)
        {
        }

        public virtual void VisitThisExpression(ThisExpression thisExpression)
        {
        }

        public virtual void VisitSequenceExpression(SequenceExpression sequenceExpression)
        {
            foreach (var e in sequenceExpression.Expressions)
            {
                VisitExpression(e);
            }
        }

        public virtual void VisitObjectExpression(ObjectExpression objectExpression)
        {
            foreach (var p in objectExpression.Properties)
            {
                if (p is Property property)
                {
                    VisitProperty(property);
                }
                else
                {
                    VisitRestElement((RestElement) p);
                }
            }
        }

        public virtual void VisitNewExpression(NewExpression newExpression)
        {
            foreach (var e in newExpression.Arguments)
            {
                VisitExpression(e.As<Expression>());
            }
            VisitExpression(newExpression.Callee);
        }

        public virtual void VisitMemberExpression(MemberExpression memberExpression)
        {
            VisitExpression(memberExpression.Object);
        }

        public virtual void VisitLogicalExpression(BinaryExpression binaryExpression)
        {
            VisitBinaryExpression(binaryExpression);
        }

        public virtual void VisitLiteral(Literal literal)
        {
        }

        public virtual void VisitIdentifier(Identifier identifier)
        {
        }

        public virtual void VisitFunctionExpression(IFunction function)
        {
            foreach (var param in function.Params)
            {
                Visit(param);
            }
            VisitBlockStatement(function.Body);
        }

        public virtual void Visit(Node node)
        {
            switch (node.Type)
            {
                case NodeType.AssignmentExpression:
                    VisitAssignmentExpression(node.As<AssignmentExpression>());
                    break;
                case NodeType.ArrayExpression:
                    VisitArrayExpression(node.As<ArrayExpression>());
                    break;
                case NodeType.BlockStatement:
                    VisitBlockStatement(node.As<BlockStatement>());
                    break;
                case NodeType.BinaryExpression:
                    VisitBinaryExpression(node.As<BinaryExpression>());
                    break;
                case NodeType.BreakStatement:
                    VisitBreakStatement(node.As<BreakStatement>());
                    break;
                case NodeType.CallExpression:
                    VisitCallExpression(node.As<CallExpression>());
                    break;
                case NodeType.CatchClause:
                    VisitCatchClause(node.As<CatchClause>());
                    break;
                case NodeType.ConditionalExpression:
                    VisitConditionalExpression(node.As<ConditionalExpression>());
                    break;
                case NodeType.ContinueStatement:
                    VisitContinueStatement(node.As<ContinueStatement>());
                    break;
                case NodeType.DoWhileStatement:
                    VisitDoWhileStatement(node.As<DoWhileStatement>());
                    break;
                case NodeType.DebuggerStatement:
                    VisitDebuggerStatement(node.As<DebuggerStatement>());
                    break;
                case NodeType.EmptyStatement:
                    VisitEmptyStatement(node.As<EmptyStatement>());
                    break;
                case NodeType.ExpressionStatement:
                    VisitExpressionStatement(node.As<ExpressionStatement>());
                    break;
                case NodeType.ForStatement:
                    VisitForStatement(node.As<ForStatement>());
                    break;
                case NodeType.ForInStatement:
                    VisitForInStatement(node.As<ForInStatement>());
                    break;
                case NodeType.FunctionDeclaration:
                    VisitFunctionDeclaration(node.As<FunctionDeclaration>());
                    break;
                case NodeType.FunctionExpression:
                    VisitFunctionExpression(node.As<FunctionExpression>());
                    break;
                case NodeType.Identifier:
                    VisitIdentifier(node.As<Identifier>());
                    break;
                case NodeType.IfStatement:
                    VisitIfStatement(node.As<IfStatement>());
                    break;
                case NodeType.Literal:
                    VisitLiteral(node.As<Literal>());
                    break;
                case NodeType.LabeledStatement:
                    VisitLabeledStatement(node.As<LabeledStatement>());
                    break;
                case NodeType.LogicalExpression:
                    VisitLogicalExpression(node.As<BinaryExpression>());
                    break;
                case NodeType.MemberExpression:
                    VisitMemberExpression(node.As<MemberExpression>());
                    break;
                case NodeType.NewExpression:
                    VisitNewExpression(node.As<NewExpression>());
                    break;
                case NodeType.ObjectExpression:
                    VisitObjectExpression(node.As<ObjectExpression>());
                    break;
                case NodeType.Program:
                    VisitProgram(node.As<Acornima.Ast.Program>());
                    break;
                case NodeType.Property:
                    VisitProperty(node.As<Property>());
                    break;
                case NodeType.RestElement:
                    VisitRestElement(node.As<RestElement>());
                    break;
                case NodeType.ReturnStatement:
                    VisitReturnStatement(node.As<ReturnStatement>());
                    break;
                case NodeType.SequenceExpression:
                    VisitSequenceExpression(node.As<SequenceExpression>());
                    break;
                case NodeType.SwitchStatement:
                    VisitSwitchStatement(node.As<SwitchStatement>());
                    break;
                case NodeType.SwitchCase:
                    VisitSwitchCase(node.As<SwitchCase>());
                    break;
                case NodeType.TemplateElement:
                    VisitTemplateElement(node.As<TemplateElement>());
                    break;
                case NodeType.TemplateLiteral:
                    VisitTemplateLiteral(node.As<TemplateLiteral>());
                    break;
                case NodeType.ThisExpression:
                    VisitThisExpression(node.As<ThisExpression>());
                    break;
                case NodeType.ThrowStatement:
                    VisitThrowStatement(node.As<ThrowStatement>());
                    break;
                case NodeType.TryStatement:
                    VisitTryStatement(node.As<TryStatement>());
                    break;
                case NodeType.UnaryExpression:
                    VisitUnaryExpression(node.As<UnaryExpression>());
                    break;
                case NodeType.UpdateExpression:
                    VisitUpdateExpression(node.As<UpdateExpression>());
                    break;
                case NodeType.VariableDeclaration:
                    VisitVariableDeclaration(node.As<VariableDeclaration>());
                    break;
                case NodeType.VariableDeclarator:
                    VisitVariableDeclarator(node.As<VariableDeclarator>());
                    break;
                case NodeType.WhileStatement:
                    VisitWhileStatement(node.As<WhileStatement>());
                    break;
                case NodeType.WithStatement:
                    VisitWithStatement(node.As<WithStatement>());
                    break;
                case NodeType.ArrayPattern:
                    VisitArrayPattern(node.As<ArrayPattern>());
                    break;
                case NodeType.AssignmentPattern:
                    VisitAssignmentPattern(node.As<AssignmentPattern>());
                    break;
                case NodeType.SpreadElement:
                    VisitSpreadElement(node.As<SpreadElement>());
                    break;
                case NodeType.ObjectPattern:
                    VisitObjectPattern(node.As<ObjectPattern>());
                    break;
                case NodeType.MetaProperty:
                    VisitMetaProperty(node.As<MetaProperty>());
                    break;
                case NodeType.Super:
                    VisitSuper(node.As<Super>());
                    break;
                case NodeType.TaggedTemplateExpression:
                    VisitTaggedTemplateExpression(node.As<TaggedTemplateExpression>());
                    break;
                case NodeType.YieldExpression:
                    VisitYieldExpression(node.As<YieldExpression>());
                    break;
                case NodeType.ArrowFunctionExpression:
                    VisitArrowFunctionExpression(node.As<ArrowFunctionExpression>());
                    break;
                case NodeType.ClassBody:
                    VistClassBody(node.As<ClassBody>());
                    break;
                case NodeType.ClassDeclaration:
                    VisitClassDeclaration(node.As<ClassDeclaration>());
                    break;
                case NodeType.ForOfStatement:
                    VisitForOfStatement(node.As<ForOfStatement>());
                    break;
                case NodeType.MethodDefinition:
                    VisitMethodDefinition(node.As<MethodDefinition>());
                    break;
                case NodeType.ImportSpecifier:
                    VisitImportSpecifier(node.As<ImportSpecifier>());
                    break;
                case NodeType.ImportDefaultSpecifier:
                    VisitImportDefaultSpecifier(node.As<ImportDefaultSpecifier>());
                    break;
                case NodeType.ImportNamespaceSpecifier:
                    VisitImportNamespaceSpecifier(node.As<ImportNamespaceSpecifier>());
                    break;
                case NodeType.ImportDeclaration:
                    VisitImportDeclaration(node.As<ImportDeclaration>());
                    break;
                case NodeType.ExportSpecifier:
                    VisitExportSpecifier(node.As<ExportSpecifier>());
                    break;
                case NodeType.ExportNamedDeclaration:
                    VisitExportNamedDeclaration(node.As<ExportNamedDeclaration>());
                    break;
                case NodeType.ExportAllDeclaration:
                    VisitExportAllDeclaration(node.As<ExportAllDeclaration>());
                    break;
                case NodeType.ExportDefaultDeclaration:
                    VisitExportDefaultDeclaration(node.As<ExportDefaultDeclaration>());
                    break;
                case NodeType.ClassExpression:
                    VisitClassExpression(node.As<ClassExpression>());
                    break;
                default:
                    VisitUnknownNode(node);
                    break;
            }
        }

        public virtual void VisitClassExpression(ClassExpression classExpression)
        {
        }

        public virtual void VisitExportDefaultDeclaration(ExportDefaultDeclaration exportDefaultDeclaration)
        {
        }

        public virtual void VisitExportAllDeclaration(ExportAllDeclaration exportAllDeclaration)
        {
        }

        public virtual void VisitExportNamedDeclaration(ExportNamedDeclaration exportNamedDeclaration)
        {
        }

        public virtual void VisitExportSpecifier(ExportSpecifier exportSpecifier)
        {
        }

        public virtual void VisitImportDeclaration(ImportDeclaration importDeclaration)
        {
        }

        public virtual void VisitImportNamespaceSpecifier(ImportNamespaceSpecifier importNamespaceSpecifier)
        {
        }

        public virtual void VisitImportDefaultSpecifier(ImportDefaultSpecifier importDefaultSpecifier)
        {
        }

        public virtual void VisitImportSpecifier(ImportSpecifier importSpecifier)
        {
        }

        public virtual void VisitMethodDefinition(MethodDefinition methodDefinitions)
        {
        }

        public virtual void VisitForOfStatement(ForOfStatement forOfStatement)
        {
            VisitExpression(forOfStatement.Right);
            Visit(forOfStatement.Left);
            VisitStatement(forOfStatement.Body);
        }

        public virtual void VisitClassDeclaration(ClassDeclaration classDeclaration)
        {
        }

        public virtual void VistClassBody(ClassBody classBody)
        {
        }

        public virtual void VisitYieldExpression(YieldExpression yieldExpression)
        {
        }

        public virtual void VisitTaggedTemplateExpression(TaggedTemplateExpression taggedTemplateExpression)
        {
        }

        public virtual void VisitSuper(Super super)
        {
        }

        public virtual void VisitMetaProperty(MetaProperty metaProperty)
        {
        }

        public virtual void VisitObjectPattern(ObjectPattern objectPattern)
        {
        }

        public virtual void VisitSpreadElement(SpreadElement spreadElement)
        {
            VisitIdentifier(spreadElement.Argument.As<Identifier>());
        }

        public virtual void VisitAssignmentPattern(AssignmentPattern assignmentPattern)
        {
        }

        public virtual void VisitArrayPattern(ArrayPattern arrayPattern)
        {
        }

        public virtual void VisitVariableDeclarator(VariableDeclarator variableDeclarator)
        {
        }

        public virtual void VisitTemplateLiteral(TemplateLiteral templateLiteral)
        {
        }

        public virtual void VisitTemplateElement(TemplateElement templateElement)
        {
        }

        public virtual void VisitRestElement(RestElement restElement)
        {
        }

        public virtual void VisitProperty(Property property)
        {
            switch (property.Kind)
            {
                case PropertyKind.Unknown:
                    break;
                case PropertyKind.Init:
                    VisitExpression(property.Value.As<Expression>());
                    break;
                case PropertyKind.Get:
                case PropertyKind.Set:
                    VisitFunctionExpression(property.Value.As<FunctionExpression>());
                    break;
                case PropertyKind.Constructor:
                    break;
                case PropertyKind.Method:
                    break;
                case PropertyKind.Property:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public virtual void VisitConditionalExpression(ConditionalExpression conditionalExpression)
        {
            VisitExpression(conditionalExpression.Test);
            VisitExpression(conditionalExpression.Consequent);
            VisitExpression(conditionalExpression.Alternate);
        }

        public virtual void VisitCallExpression(CallExpression callExpression)
        {
            VisitExpression(callExpression.Callee);

            foreach (var arg in callExpression.Arguments)
            {
                VisitExpression(arg.As<Expression>());
            }
        }

        public virtual void VisitBinaryExpression(BinaryExpression binaryExpression)
        {
            VisitExpression(binaryExpression.Left.As<Expression>());
            VisitExpression(binaryExpression.Right.As<Expression>());
        }

        public virtual void VisitArrayExpression(ArrayExpression arrayExpression)
        {
            foreach (var expr in arrayExpression.Elements)
            {
                VisitExpression(expr.As<Expression>());
            }
        }

        public virtual void VisitAssignmentExpression(AssignmentExpression assignmentExpression)
        {
            VisitExpression(assignmentExpression.Left.As<Expression>());
            VisitExpression(assignmentExpression.Right.As<Expression>());
        }

        public virtual void VisitContinueStatement(ContinueStatement continueStatement)
        {
        }

        public virtual void VisitBreakStatement(BreakStatement breakStatement)
        {
        }

        public virtual void VisitBlockStatement(Node blockStatement)
        {
            foreach (var statement in blockStatement.ChildNodes)
            {
                VisitStatement(statement.As<Statement>());
            }
        }
    }
}
