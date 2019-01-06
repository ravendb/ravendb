using System;
using System.Collections.Generic;
using System.Linq;
using Esprima;
using Esprima.Ast;
using Jint;

namespace Raven.Server.Documents.Indexes.Static
{
    public class EsprimaVisitor
    {
        public virtual void VisitProgram(Esprima.Ast.Program program)
        {
            foreach (var statement in program.Body)
            {
                VisitStatement((Statement)statement);
            }
        }

        public virtual void VisitStatement(Statement statement)
        {
            if (statement == null)
                return;

            switch (statement.Type)
            {
                case Nodes.BlockStatement:
                    VisitBlockStatement(statement.As<BlockStatement>());
                    break;
                case Nodes.BreakStatement:
                    VisitBreakStatement(statement.As<BreakStatement>());
                    break;
                case Nodes.ContinueStatement:
                    VisitContinueStatement(statement.As<ContinueStatement>());
                    break;
                case Nodes.DoWhileStatement:
                    VisitDoWhileStatement(statement.As<DoWhileStatement>());
                    break;
                case Nodes.DebuggerStatement:
                    VisitDebuggerStatement(statement.As<DebuggerStatement>());
                    break;
                case Nodes.EmptyStatement:
                    VisitEmptyStatement(statement.As<EmptyStatement>());
                    break;
                case Nodes.ExpressionStatement:
                    VisitExpressionStatement(statement.As<ExpressionStatement>());
                    break;
                case Nodes.ForStatement:
                    VisitForStatement(statement.As<ForStatement>());
                    break;
                case Nodes.ForInStatement:
                    VisitForInStatement(statement.As<ForInStatement>());
                    break;
                case Nodes.FunctionDeclaration:
                    VisitFunctionDeclaration(statement.As<FunctionDeclaration>());
                    break;
                case Nodes.IfStatement:
                    VisitIfStatement(statement.As<IfStatement>());
                    break;
                case Nodes.LabeledStatement:
                    VisitLabeledStatement(statement.As<LabeledStatement>());
                    break;
                case Nodes.ReturnStatement:
                    VisitReturnStatement(statement.As<ReturnStatement>());
                    break;
                case Nodes.SwitchStatement:
                    VisitSwitchStatement(statement.As<SwitchStatement>());
                    break;
                case Nodes.ThrowStatement:
                    VisitThrowStatement(statement.As<ThrowStatement>());
                    break;
                case Nodes.TryStatement:
                    VisitTryStatement(statement.As<TryStatement>());
                    break;
                case Nodes.VariableDeclaration:
                    VisitVariableDeclaration(statement.As<VariableDeclaration>());
                    break;
                case Nodes.WhileStatement:
                    VisitWhileStatement(statement.As<WhileStatement>());
                    break;
                case Nodes.WithStatement:
                    VisitWithStatement(statement.As<WithStatement>());
                    break;
                case Nodes.Program:
                    VisitProgram(statement.As<Esprima.Ast.Program>());
                    break;
                case Nodes.CatchClause:
                    VisitCatchClause(statement.As<CatchClause>());
                    break;
                default:
                    VisitUnknownNode(statement);
                    break;
            }
        }

        public virtual void VisitUnknownNode(INode node)
        {
            throw new NotImplementedException($"ESprima visitor doesn't support nodes of type {node.Type}, you can override VisitUnknownNode to handle this case.");
        }

        public virtual void VisitUnknownObject(object obj)
        {
            throw new NotImplementedException($"ESprima visitor doesn't support object of type {obj?.GetType()}, you can override VisitUnknownObject to handle this case.");
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
            VisitExpression(switchCase.Test);
            foreach (var s in switchCase.Consequent)
            {
                //In most cases it is going to be statment
                if (s is Statement statment)
                {
                    VisitStatement(statment);
                }
                else if (s is INode node)
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
            if(returnStatement.Argument == null)
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
                if (forStatement.Init.Type == Nodes.VariableDeclaration)
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
            Identifier identifier = forInStatement.Left.Type == Nodes.VariableDeclaration
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
            switch (expression.Type)
            {
                case Nodes.AssignmentExpression:
                    VisitAssignmentExpression(expression.As<AssignmentExpression>());
                    break;
                case Nodes.ArrayExpression:
                    VisitArrayExpression(expression.As<ArrayExpression>());
                    break;
                case Nodes.BinaryExpression:
                    VisitBinaryExpression(expression.As<BinaryExpression>());
                    break;
                case Nodes.CallExpression:
                    VisitCallExpression(expression.As<CallExpression>());
                    break;
                case Nodes.ConditionalExpression:
                    VisitConditionalExpression(expression.As<ConditionalExpression>());
                    break;
                case Nodes.FunctionExpression:
                    VisitFunctionExpression(expression.As<IFunction>());
                    break;
                case Nodes.Identifier:
                    VisitIdentifier(expression.As<Identifier>());
                    break;
                case Nodes.Literal:
                    VisitLiteral(expression.As<Literal>());
                    break;
                case Nodes.LogicalExpression:
                    VisitLogicalExpression(expression.As<BinaryExpression>());
                    break;
                case Nodes.MemberExpression:
                    VisitMemberExpression(expression.As<MemberExpression>());
                    break;
                case Nodes.NewExpression:
                    VisitNewExpression(expression.As<NewExpression>());
                    break;
                case Nodes.ObjectExpression:
                    VisitObjectExpression(expression.As<ObjectExpression>());
                    break;
                case Nodes.SequenceExpression:
                    VisitSequenceExpression(expression.As<SequenceExpression>());
                    break;
                case Nodes.ThisExpression:
                    VisitThisExpression(expression.As<ThisExpression>());
                    break;
                case Nodes.UpdateExpression:
                    VisitUpdateExpression(expression.As<UpdateExpression>());
                    break;
                case Nodes.UnaryExpression:
                    VisitUnaryExpression(expression.As<UnaryExpression>());
                    break;
                case Nodes.ArrowFunctionExpression:
                    VisitArrowFunctionExpression(expression.As<ArrowFunctionExpression>());
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
                    ? new BlockStatement(new List<StatementListItem> { new ReturnStatement(arrowFunctionExpression.Body.As<Expression>()) })
                    : arrowFunctionExpression.Body.As<BlockStatement>();
            var func = new FunctionExpression(new Identifier(null),
                arrowFunctionExpression.Params,
                statement,
                false,
                new HoistingScope(),
                StrictModeScope.IsStrictModeCode);
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
               VisitProperty(p); 
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

        public virtual void Visit(INode node)
        {
            switch (node.Type)
            {
                case Nodes.AssignmentExpression:
                    VisitAssignmentExpression(node.As<AssignmentExpression>());
                    break;
                case Nodes.ArrayExpression:
                    VisitArrayExpression(node.As<ArrayExpression>());
                    break;
                case Nodes.BlockStatement:
                    VisitBlockStatement(node.As<BlockStatement>());
                    break;
                case Nodes.BinaryExpression:
                    VisitBinaryExpression(node.As<BinaryExpression>());
                    break;
                case Nodes.BreakStatement:
                    VisitBreakStatement(node.As<BreakStatement>());
                    break;
                case Nodes.CallExpression:
                    VisitCallExpression(node.As<CallExpression>());
                    break;
                case Nodes.CatchClause:
                    VisitCatchClause(node.As<CatchClause>());
                    break;
                case Nodes.ConditionalExpression:
                    VisitConditionalExpression(node.As<ConditionalExpression>());
                    break;
                case Nodes.ContinueStatement:
                    VisitContinueStatement(node.As<ContinueStatement>());
                    break;
                case Nodes.DoWhileStatement:
                    VisitDoWhileStatement(node.As<DoWhileStatement>());
                    break;
                case Nodes.DebuggerStatement:
                    VisitDebuggerStatement(node.As<DebuggerStatement>());
                    break;
                case Nodes.EmptyStatement:
                    VisitEmptyStatement(node.As<EmptyStatement>());
                    break;
                case Nodes.ExpressionStatement:
                    VisitExpressionStatement(node.As<ExpressionStatement>());
                    break;
                case Nodes.ForStatement:
                    VisitForStatement(node.As<ForStatement>());
                    break;
                case Nodes.ForInStatement:
                    VisitForInStatement(node.As<ForInStatement>());
                    break;
                case Nodes.FunctionDeclaration:
                    VisitFunctionDeclaration(node.As<FunctionDeclaration>());
                    break;
                case Nodes.FunctionExpression:
                    VisitFunctionExpression(node.As<FunctionExpression>());
                    break;
                case Nodes.Identifier:
                    VisitIdentifier(node.As<Identifier>());
                    break;
                case Nodes.IfStatement:
                    VisitIfStatement(node.As<IfStatement>());
                    break;
                case Nodes.Literal:
                    VisitLiteral(node.As<Literal>());
                    break;
                case Nodes.LabeledStatement:
                    VisitLabeledStatement(node.As<LabeledStatement>());
                    break;
                case Nodes.LogicalExpression:
                    VisitLogicalExpression(node.As<BinaryExpression>());
                    break;
                case Nodes.MemberExpression:
                    VisitMemberExpression(node.As<MemberExpression>());
                    break;
                case Nodes.NewExpression:
                    VisitNewExpression(node.As<NewExpression>());
                    break;
                case Nodes.ObjectExpression:
                    VisitObjectExpression(node.As<ObjectExpression>());
                    break;
                case Nodes.Program:
                    VisitProgram(node.As<Esprima.Ast.Program>());
                    break;
                case Nodes.Property:
                    VisitProperty(node.As<Property>());
                    break;
                case Nodes.RestElement:
                    VisitRestElement(node.As<RestElement>());
                    break;
                case Nodes.ReturnStatement:
                    VisitReturnStatement(node.As<ReturnStatement>());
                    break;
                case Nodes.SequenceExpression:
                    VisitSequenceExpression(node.As<SequenceExpression>());
                    break;
                case Nodes.SwitchStatement:
                    VisitSwitchStatement(node.As<SwitchStatement>());
                    break;
                case Nodes.SwitchCase:
                    VisitSwitchCase(node.As<SwitchCase>());
                    break;
                case Nodes.TemplateElement:
                    VisitTemplateElement(node.As<TemplateElement>());
                    break;
                case Nodes.TemplateLiteral:
                    VisitTemplateLiteral(node.As<TemplateLiteral>());
                    break;
                case Nodes.ThisExpression:
                    VisitThisExpression(node.As<ThisExpression>());
                    break;
                case Nodes.ThrowStatement:
                    VisitThrowStatement(node.As<ThrowStatement>());
                    break;
                case Nodes.TryStatement:
                    VisitTryStatement(node.As<TryStatement>());
                    break;
                case Nodes.UnaryExpression:
                    VisitUnaryExpression(node.As<UnaryExpression>());
                    break;
                case Nodes.UpdateExpression:
                    VisitUpdateExpression(node.As<UpdateExpression>());
                    break;
                case Nodes.VariableDeclaration:
                    VisitVariableDeclaration(node.As<VariableDeclaration>());
                    break;
                case Nodes.VariableDeclarator:
                    VisitVariableDeclarator(node.As<VariableDeclarator>());
                    break;
                case Nodes.WhileStatement:
                    VisitWhileStatement(node.As<WhileStatement>());
                    break;
                case Nodes.WithStatement:
                    VisitWithStatement(node.As<WithStatement>());
                    break;
                case Nodes.ArrayPattern:
                    VisitArrayPattern(node.As<ArrayPattern>());
                    break;
                case Nodes.AssignmentPattern:
                    VisitAssignmentPattern(node.As<AssignmentPattern>());
                    break;
                case Nodes.SpreadElement:
                    VisitSpreadElement(node.As<SpreadElement>());
                    break;
                case Nodes.ObjectPattern:
                    VisitObjectPattern(node.As<ObjectPattern>());
                    break;
                case Nodes.ArrowParameterPlaceHolder:
                    VisitArrowParameterPlaceHolder(node.As<ArrowParameterPlaceHolder>());
                    break;
                case Nodes.MetaProperty:
                    VisitMetaProperty(node.As<MetaProperty>());
                    break;
                case Nodes.Super:
                    VisitSuper(node.As<Super>());
                    break;
                case Nodes.TaggedTemplateExpression:
                    VisitTaggedTemplateExpression(node.As<TaggedTemplateExpression>());
                    break;
                case Nodes.YieldExpression:
                    VisitYieldExpression(node.As<YieldExpression>());
                    break;
                case Nodes.ArrowFunctionExpression:
                    VisitArrowFunctionExpression(node.As<ArrowFunctionExpression>());
                    break;
                case Nodes.ClassBody:
                    VistClassBody(node.As<ClassBody>());
                    break;
                case Nodes.ClassDeclaration:
                    VisitClassDeclaration(node.As<ClassDeclaration>());
                    break;
                case Nodes.ForOfStatement:
                    VisitForOfStatement(node.As<ForOfStatement>());
                    break;
                case Nodes.MethodDefinition:
                    VisitMethodDefinition(node.As<MethodDefinition>());
                    break;
                case Nodes.ImportSpecifier:
                    VisitImportSpecifier(node.As<ImportSpecifier>());
                    break;
                case Nodes.ImportDefaultSpecifier:
                    VisitImportDefaultSpecifier(node.As<ImportDefaultSpecifier>());
                    break;
                case Nodes.ImportNamespaceSpecifier:
                    VisitImportNamespaceSpecifier(node.As<ImportNamespaceSpecifier>());
                    break;
                case Nodes.ImportDeclaration:
                    VisitImportDeclaration(node.As<ImportDeclaration>());
                    break;
                case Nodes.ExportSpecifier:
                    VisitExportSpecifier(node.As<ExportSpecifier>());
                    break;
                case Nodes.ExportNamedDeclaration:
                    VisitExportNamedDeclaration(node.As<ExportNamedDeclaration>());
                    break;
                case Nodes.ExportAllDeclaration:
                    VisitExportAllDeclaration(node.As<ExportAllDeclaration>());
                    break;
                case Nodes.ExportDefaultDeclaration:
                    VisitExportDefaultDeclaration(node.As<ExportDefaultDeclaration>());
                    break;
                case Nodes.ClassExpression:
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

        public virtual void VisitArrowParameterPlaceHolder(ArrowParameterPlaceHolder arrowParameterPlaceHolder)
        {
        }

        public virtual void VisitObjectPattern(ObjectPattern objectPattern)
        {         
        }

        public virtual void VisitSpreadElement(SpreadElement spreadElement)
        {
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
                case PropertyKind.Init:
                case PropertyKind.Data:
                    VisitExpression(property.Value.As<Expression>());
                    break;
                case PropertyKind.None:
                    break;
                case PropertyKind.Set:
                case PropertyKind.Get:
                    VisitFunctionExpression(property.Value.As<IFunction>());
                    break;
                case PropertyKind.Constructor:
                    break;
                case PropertyKind.Method:
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
            if (callExpression.Cached == false)
            {
                foreach (var arg in callExpression.Arguments)
                {
                    VisitExpression(arg.As<Expression>());
                }
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

        public virtual void VisitBlockStatement(BlockStatement BlockStatement)
        {
            foreach (var statement in BlockStatement.Body)
            {
                VisitStatement(statement.As<Statement>());
            }
        }
    }
}
