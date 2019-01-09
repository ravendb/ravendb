using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Esprima;
using Microsoft.Extensions.Primitives;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Extensions;
// ReSharper disable ConditionIsAlwaysTrueOrFalse

namespace Raven.Server.Documents.Queries.Parser
{
    public class QueryParser
    {
        private static readonly string[] OperatorStartMatches = { ">=", "<=", "<>", "<", ">", "==", "=", "!=", "BETWEEN", "IN", "ALL IN", "(" };
        private static readonly string[] BinaryOperators = { "OR", "AND" };
        private static readonly string[] StaticValues = { "true", "false", "null" };
        private static readonly string[] OrderByOptions = { "ASC", "DESC", "ASCENDING", "DESCENDING" };
        private static readonly string[] OrderByAsOptions = { "string", "long", "double", "alphaNumeric" };


        private int _counter;
        private int _depth;
        private NextTokenOptions _state = NextTokenOptions.Parenthesis;

        private int _statePos;

        public QueryScanner Scanner = new QueryScanner();
        private Dictionary<StringSegment, SynteticWithQuery> _synteticWithQueries;
        private struct SynteticWithQuery
        {
            public FieldExpression Path;
            public QueryExpression Filter;
            public FieldExpression Project;
            public bool IsEdge;
            public bool ImplicitAlias;

            public SynteticWithQuery(FieldExpression path, QueryExpression filter, FieldExpression project, bool isEdge, bool implicitAlias)
            {
                Path = path;
                Filter = filter;
                Project = project;
                IsEdge = isEdge;
                ImplicitAlias = implicitAlias;
            }
        }

        public void Init(string q)
        {
            _depth = 0;
            Scanner.Init(q);
        }

        public Query Parse(QueryType queryType = QueryType.Select, bool recursive = false)
        {
            if(!TryParse(out var query, out var message, queryType, recursive))
                ThrowParseException(message);

            return query;
        }

        public bool TryParse(out Query query,out string message, QueryType queryType = QueryType.Select, bool recursive = false)
        {
            query = new Query
            {
                QueryText = Scanner.Input
            };
            message = string.Empty;

            while (Scanner.TryScan("DECLARE"))
            {
                var (name, func) = DeclaredFunction();

                if (query.TryAddFunction(name, func) == false)
                {
                    message = $"{name} function was declared multiple times";
                    return false;
                }
            }

            while (Scanner.TryScan("WITH"))
            {
                if (recursive)
                {
                    message = "With clause is not allow inside inner query";
                    return false;
                }

                WithClause(query);
            }

            if (Scanner.TryScan("MATCH") == false)
            {
                if (query.GraphQuery != null)
                {
                    message = "Missing a 'match' clause after 'with' clause";
                    return false;
                }

                if (!TryParseFromClause(out var fromClause, out message))
                    return false;

                query.From = fromClause;

                if (Scanner.TryScan("GROUP BY"))
                    query.GroupBy = GroupBy();

                if (Scanner.TryScan("WHERE") && Expression(out query.Where) == false)
                {
                    message = "Unable to parse WHERE clause";
                    return false;
                }
            }
            else
            {
                if (query.GraphQuery == null)
                    query.GraphQuery = new GraphQuery();

                if (BinaryGraph(query.GraphQuery, out var op) == false)
                {
                    message = "Unexpected input when trying to parse the MATCH clause";
                    return false;
                }

                query.GraphQuery.MatchClause = op;

                if (_synteticWithQueries != null)
                {
                    foreach (var (alias, sq) in _synteticWithQueries)
                    {
                        if (sq.IsEdge)
                        {
                            var with = new WithEdgesExpression(sq.Filter, sq.Path, sq.Project, null);
                            query.TryAddWithEdgePredicates(with, alias);
                        }
                        else
                        {
                            query.TryAddWithClause(new Query
                            {
                                From = new FromClause
                                {
                                    From = sq.Path,
                                },
                                Where = sq.Filter
                            }, alias);
                        }
                    }
                    _synteticWithQueries.Clear();
                }

                if (Scanner.TryScan("WHERE") && Expression(out query.GraphQuery.Where) == false)
                {
                    message = "Unable to parse MATCH's WHERE clause";
                    return false;
                }
            }

            if (Scanner.TryScan("ORDER BY"))
                query.OrderBy = OrderBy();

            if (Scanner.TryScan("LOAD"))
                query.Load = SelectClauseExpressions("LOAD", false);

            switch (queryType)
            {
                case QueryType.Select:
                    if (Scanner.TryScan("SELECT"))
                        query.Select = SelectClause("SELECT", query);
                    if (Scanner.TryScan("INCLUDE"))
                        query.Include = IncludeClause();
                    break;
                case QueryType.Update:

                    if (query.GraphQuery != null)
                    {
                        message = "Update operations cannot use graph queries";
                        return false;
                    }

                    if (Scanner.TryScan("UPDATE") == false)
                    {
                        message = "Update operations must end with UPDATE clause";
                        return false;
                    }

                    var functionStart = Scanner.Position;
                    if (Scanner.FunctionBody() == false)
                    {
                        message = "Update clause must have a single function body";
                        return false;
                    }

                    query.UpdateBody = Scanner.Input.Substring(functionStart, Scanner.Position - functionStart);
                    try
                    {
                        // validate the js code
                        ValidateScript("function test()" + query.UpdateBody);
                    }
                    catch (Exception e)
                    {
                        var msg = AddLineAndColumnNumberToErrorMessage(e, "Update clause contains invalid script");
                        throw new InvalidQueryException(msg, Scanner.Input, null, e);
                    }
                    break;
                default:
                    ThrowUnknownQueryType(queryType);
                    break;
            }

            Paging(out query.Offset, out query.Limit);

            if (recursive == false && Scanner.AtEndOfInput() == false)
            {
                message = "Expected end of query";
                return false;
            }

            return true;
        }


        private void Paging(out ValueExpression offset, out ValueExpression limit)
        {
            offset = null;
            limit = null;

            if (Scanner.TryScan("LIMIT"))
            {
                if (Value(out var first) == false)
                    throw new InvalidQueryException("Limit must contain a value", Scanner.Input, null);

                if (Scanner.TryScan(","))
                {
                    if (Value(out var second) == false)
                        throw new InvalidQueryException("Limit must contain a second value", Scanner.Input, null);

                    offset = first;
                    limit = second;
                }
                else
                {
                    limit = first;
                }
            }

            if (Scanner.TryScan("OFFSET"))
            {
                if (offset != null)
                    throw new InvalidQueryException("Cannot use 'offset' after 'limit $skip,$take'", Scanner.Input, null);

                if (Value(out var second) == false)
                    throw new InvalidQueryException("Offset must contain a value", Scanner.Input, null);

                offset = second;
            }
        }

        private void WithClause(Query q)
        {
            if (Scanner.TryScan("EDGES"))
            {
                Scanner.Identifier();

                var (with, alias) = WithEdges();
                q.TryAddWithEdgePredicates(with, alias);
            }
            else
            {
                var (expr, alias) = With();
                q.TryAddWithClause(expr, alias);
            }
        }

        private FieldExpression GetEdgePath()
        {
            FieldExpression f = null;
            if (Scanner.TryScan('('))
            {
                Field(out f); // okay if false

                if (Scanner.TryScan(')') == false)
                    ThrowParseException("With edges(<identifier>) was not closed with ')'");
            }

            return f;
        }

        private (WithEdgesExpression Expression, StringSegment Allias) WithEdges()
        {
            var edgeField = GetEdgePath();

            if (Scanner.TryScan('{') == false)
            {
                if (Alias(true, out var shortAlias) && shortAlias.HasValue)
                {
                    var shortWithEdges = new WithEdgesExpression(null, edgeField, null, null);
                    return (shortWithEdges, shortAlias.Value);
                }

                throw new InvalidQueryException("With edges should be followed with '{' ", Scanner.Input, null);
            }

            QueryExpression qe = null;
            if (Scanner.TryScan("WHERE") && (Expression(out qe) == false || qe == null))
                ThrowParseException("Unable to parse WHERE clause of an 'With Edges' clause");

            List<(QueryExpression Expression, OrderByFieldType OrderingType, bool Ascending)> orderBy = null;
            if (Scanner.TryScan("ORDER BY"))
            {
                orderBy = OrderBy();
            }

            FieldExpression project = null;
            if (Scanner.TryScan("SELECT"))
            {
                var selectClause = SelectClauseExpressions("SELECT", false);
                if(selectClause.Count != 1 || selectClause[0].Item2 != null)
                    ThrowParseException("Unable to parse SELECT clause of an 'With Edges' clause, must contain only a singel field and no aliases");
                project = selectClause[0].Item1 as FieldExpression;
                if (project == null)
                    ThrowParseException("Unable to parse SELECT clause of an 'With Edges' clause, projection must be field reference");
            }


            if (Scanner.TryScan('}') == false)
                throw new InvalidQueryException("With clause contains invalid body", Scanner.Input, null);

            if (Alias(true, out var alias) == false || alias.HasValue == false)
                throw new InvalidQueryException("With clause must contain alias but none was provided", Scanner.Input, null);

            var wee = new WithEdgesExpression(qe, edgeField, project, orderBy);
            return (wee, alias.Value);
        }

        public static readonly string[] EdgeOps = new[] { "<-", ">", "[", "-", "<", "(", "recursive" };

        private readonly Dictionary<StringSegment, int> _duplicateImplicitAliasCount = new Dictionary<StringSegment, int>();

        private static void EnsureKey(Dictionary<StringSegment, int> dict, StringSegment key)
        {
            if(!dict.TryGetValue(key,out _))
                dict.Add(key,0);
        }

        private bool GraphAlias(GraphQuery gq,bool isEdge, StringSegment implicitPrefix, out StringSegment alias)
        {
            // Orders as o where id() 'orders/1-A'
            // Lines where Name = 'Chang' select Product

            SynteticWithQuery prev = default;
            alias = default;
            var start = Scanner.Position;

            if (Scanner.TryScan("from"))
            {
                ThrowParseException("Unexpected 'from' clause. The correct syntax to use in pattern match node/edge expressions is to omit the 'from' keyword and then write the expression inside the node/edge.");
            }

            if (Scanner.TryPeek("INDEX") && TryParseExpressionAfterFromKeyword(out var fromClause, out _))
            {
                if (isEdge)
                    throw new InvalidQueryException("Unexpected index query expression - they are forbidden to be inside edge query elements.");

                var query = new Query
                {
                    From = fromClause
                };

                if (Scanner.TryScan("WHERE") && Expression(out query.Where) == false)
                {                
                    ThrowParseException("Unable to parse WHERE clause in the nod/edge expression.");
                }
                
                if (Scanner.TryScan("ORDER BY"))
                {                    
                    ThrowParseException("ORDER BY clause is not supported in the nod/edge expression.");
                }

                if (Scanner.TryScan("LOAD"))
                    query.Load = SelectClauseExpressions("LOAD", false);

                if (Scanner.TryScan("SELECT"))
                    throw new InvalidQueryException("SELECT clause is allowed only in edge expressions");

                alias = fromClause.Alias ?? new StringSegment($"index_{string.Join('_', fromClause.From.Compound).Replace('/', '_')}_{++_counter}");
                gq.WithDocumentQueries.TryAdd(alias, query);
            }
            else
            {
                if (Field(out var collection) == false)
                {
                    if (isEdge)
                    {
                        ThrowParseException("Unable to read edge alias");
                    }                 

                    if (Scanner.TryPeek(')')) // anonymous alias () accepts everything
                    {
                        alias = "__alias" + (++_counter);
                        AddWithQuery(new FieldExpression(new List<StringSegment>()), null, alias, null, false, start, false);
                        return true;
                    }

                    alias = default;
                    return false;
                }

                if (Scanner.TryPeek(')'))
                {
                    alias = GenerateAlias(implicitPrefix, collection);

                    if (gq.HasAlias(alias))
                        return true;

                    if (_synteticWithQueries?.TryGetValue(collection.FieldValue, out prev) == true && !prev.IsEdge)
                        return true;

                    AddWithQuery(collection, null, alias, null, isEdge, start, true);
                    return true;
                }

                if (collection.FieldValue == "_") // (_ as e) anonymous alias
                    collection = new FieldExpression(new List<StringSegment>());

                if (Alias(true, out var maybeAlias) == false)
                {
                    if (gq.HasAlias(collection.FieldValue) ||
                        isEdge == false && _synteticWithQueries?.ContainsKey(collection.FieldValue) == true
                    )
                    {
                        alias = collection.FieldValue;
                    }
                    //by this point we know AS' keyword is not the next token because Alias() checks for that
                    else if (Scanner.TryPeekNextToken(c => c != ')' && c != ']', out var token) && token.IsIdentifier())
                    {
                        if (isEdge && !token.Equals("where",StringComparison.OrdinalIgnoreCase) && !token.Equals("select",StringComparison.OrdinalIgnoreCase))
                        {
                            ThrowInvalidSyntaxMissingAs(isEdge, collection, token); //this will throw
                        }
                        else if (!isEdge && token.Equals("select", StringComparison.OrdinalIgnoreCase))
                        {
                            ThrowInvalidSyntaxSelectIsForbidden(collection);
                        }
                        else if (!isEdge && !token.Equals("where", StringComparison.OrdinalIgnoreCase))
                        {
                            ThrowInvalidSyntaxMissingAs(isEdge, collection, token);
                        }
                        else
                        {
                            alias = GenerateAlias(implicitPrefix, collection);
                        }
                    }
                    else
                    {
                        alias = GenerateAlias(implicitPrefix, collection);
                    }
                }
                else if (maybeAlias.Value == "_")
                {
                    alias = "__alias" + (++_counter);
                }
                else
                {
                    alias = maybeAlias.Value;
                }
            

            QueryExpression filter = null;
            if (Scanner.TryScan("WHERE"))
            {
                if (Expression(out filter) == false)
                    throw new InvalidQueryException($"Failed to parse filter expression for: {alias}", Scanner.Input, null);
            }

            FieldExpression project = null;
            if (Scanner.TryScan("SELECT"))
            {
                if (!isEdge)
                    throw new InvalidQueryException("Select expression is allowed only inside an edge query.");

                var fields = SelectClauseExpressions("SELECT", false);
                if (fields.Count != 1)
                    throw new InvalidQueryException("Select expression inside graph query for '" + alias + "' must have excatly one projected field", Scanner.Input, null);

                if (fields[0].Item2 != null)
                    throw new InvalidQueryException("Select expression inside graph query for '" + alias + "' cannot define alias for projected field", Scanner.Input, null);

                project = (FieldExpression)fields[0].Item1;
            }

            if (gq.HasAlias(alias))
                return true;
        
            AddWithQuery(collection, project, alias, filter, isEdge, start, maybeAlias == null);
                }
            return true;
        }

        private void ThrowInvalidSyntaxMissingAs(bool isEdge, FieldExpression collection, StringSegment token)
        {
            var msg =
                $"Inside a {(isEdge ? "edge" : "node")} I found an expression in a form of '{collection.FieldValue} {token}'. This is invalid syntax, perhaps '{token}' is an alias? If so, the correct syntax to specify an alias for an edge or a node is '{collection.FieldValue} as {token}'";
            throw new InvalidQueryException(msg, Scanner.Input);
        }

        private void ThrowInvalidSyntaxSelectIsForbidden(FieldExpression collection)
        {
            var msg =
                $"Inside a node I found an expression in a form of '{collection.FieldValue} select <SELECT FIELDS>'. Select clauses are forbidden in node query elements.";
            throw new InvalidQueryException(msg, Scanner.Input);
        }

        private StringSegment GenerateAlias(StringSegment implicitPrefix, FieldExpression collection)
        {
            StringSegment alias;
            if (implicitPrefix.Length > 0)
            {
                EnsureKey(_duplicateImplicitAliasCount, implicitPrefix);
                var duplicateCount = ++_duplicateImplicitAliasCount[implicitPrefix];
                alias = implicitPrefix + "_" + collection.FieldValue + (duplicateCount >= 2 ? "_" + duplicateCount : string.Empty);
            }
            else
            {
                alias = collection.FieldValue;
            }

            return alias;
        }

        private void AddWithQuery(FieldExpression path, FieldExpression project, StringSegment alias, QueryExpression filter, bool isEdge, int start, bool implicitAlias)
        {
            if (_synteticWithQueries == null)
                _synteticWithQueries = new Dictionary<StringSegment, SynteticWithQuery>(StringSegmentComparer.Ordinal);

            if (_synteticWithQueries.TryGetValue(alias, out var existing))
            {
                if (existing.IsEdge != isEdge)
                    ThrowDuplicateAliasWithoutSameBody(start);

                if (path.Equals(existing.Path) == false)
                {
                    if(path.Compound.Count != 0)
                    {
                        ThrowDuplicateAliasWithoutSameBody(start);
                    }
                    if(existing.Path.Compound.Count == 0)
                    {
                        existing.Path = path;
                    }
                }

                if ((filter != null) != (existing.Filter != null) || 
                    (project != null) != (existing.Project != null))
                    ThrowDuplicateAliasWithoutSameBody(start);

                if (filter != null && filter.Equals(existing.Filter) == false ||
                    project != null && project.Equals(existing.Project) == false)
                    ThrowDuplicateAliasWithoutSameBody(start);

                return;
            }

            _synteticWithQueries.Add(alias, new SynteticWithQuery(path, filter, project, isEdge, implicitAlias));
        }

        private void ThrowDuplicateAliasWithoutSameBody(int start)
        {
            throw new InvalidQueryException("Duplicated alias  was found with a different definition than previous defined: " +
                new StringSegment(Scanner.Input, start, Scanner.Position - start),
                 Scanner.Input, null);
        }

        private void ThrowRedefineSameAnonymousAlias(string alias, int start)
        {
            throw new InvalidQueryException("Implicit alias '" + alias +"' was redefined in the same query, use an explicit '" + alias + " as e', instead. " + 
                new StringSegment(Scanner.Input, start, Scanner.Position - start),
                 Scanner.Input, null);
        }


        private bool BinaryGraph(GraphQuery gq, out QueryExpression op)
        {
            if (GraphOperation(gq, out op) == false)
                return false;

            if (Scanner.TryScan(BinaryOperators, out var found) == false)
                return true; // found simple

            var negate = Scanner.TryScan("NOT");
            var type = found == "OR"
                ? OperatorType.Or
                : OperatorType.And;

            _state = NextTokenOptions.Parenthesis;

            var parenthesis = Scanner.TryPeek('(');

            if (BinaryGraph(gq, out var right) == false)
                ThrowParseException($"Failed to find second part of {type} expression");

            return TrySimplifyBinaryExpression(right, type, negate, parenthesis, ref op);
        }

        private bool GraphOperation(GraphQuery gq, out QueryExpression op)
        {
            if (Scanner.TryScan('(') == false)
                throw new InvalidQueryException("MATCH operator expected a '(', but didn't get it.", Scanner.Input, null);


            if (GraphAlias(gq, false, default, out var alias) == false)
            {
                if (BinaryGraph(gq, out op) == false)
                {
                    throw new InvalidQueryException("Invalid expression in MATCH", Scanner.Input, null);
                }

                if (Scanner.TryScan(')') == false)
                    throw new InvalidQueryException("Missing ')' in MATCH", Scanner.Input, null);
                return true;
            }

            if (Scanner.TryScan(')') == false)
                throw new InvalidQueryException("MATCH operator expected a ')' after reading: " + alias, Scanner.Input, null);

            var list = new List<MatchPath>();

            list.Add(new MatchPath
            {
                Alias = alias,
                EdgeType = EdgeType.Right
            });

            return ProcessEdges(gq, out op, alias, list, allowRecursive: true, foundDash: false);
        }


        private bool ProcessEdges(GraphQuery gq, out QueryExpression op, StringSegment alias, List<MatchPath> list, bool allowRecursive, bool foundDash)
        {
            var expectNode = false;
            MatchPath last;
            var foundLeftArrow = false;
            while (true)

            {
                if (Scanner.TryScan(EdgeOps, out var found))
                {
                    switch (found)
                    {
                        case "-":
                            foundDash = true;
                            foundLeftArrow = false;
                            continue;
                        case "[":
                            if (foundDash == false)
                                throw new InvalidQueryException("Got '[' when expected '-', did you forget to add '-[' ?", Scanner.Input, null);

                            var startingPos = Scanner.Position - 1;

                            if (GraphAlias(gq, true, alias, out alias) == false)
                                throw new InvalidQueryException("MATCH identifier after '-['", Scanner.Input, null);

                            var endingPos = Scanner.Position + 1;

                            if (expectNode)
                            {
                                ThrowExpectedNodeButFoundEdge(alias, Scanner.Input.Substring(startingPos, endingPos - startingPos),Scanner.Input);
                            }

                            if (Scanner.TryScan(']') == false)
                                throw new InvalidQueryException("MATCH operator expected a ']' after reading: " + alias, Scanner.Input, null);

                            if (foundLeftArrow && //before current edge we had '<-' operator
                                Scanner.TryPeek("->"))
                            {
                                throw new InvalidQueryException($"The edge with alias '{alias}' has arrow operators ('->') in both left and right directions. This is invalid syntax, edge elements should have either '-[edge expression]->' or '<-[edge expression]-' format.");
                            }

                            list.Add(new MatchPath
                            {
                                Alias = alias,
                                EdgeType = list[list.Count - 1].EdgeType,
                                IsEdge = true
                            });
                            foundLeftArrow = false;
                            expectNode = true; //after each edge we expect a node
                            break;
                        case "<-":

                            last = list[list.Count - 1];
                            list[list.Count - 1] = new MatchPath
                            {
                                Alias = last.Alias,
                                IsEdge = last.IsEdge,
                                EdgeType = EdgeType.Left,
                                Recursive = last.Recursive
                            };

                            foundDash = true;
                            foundLeftArrow = true;

                            continue;
                        case "<":
                            throw new InvalidQueryException("Got unexpected '<', did you forget to add '->' ?", Scanner.Input, null);
                        case ">":
                            if (foundDash == false)
                                throw new InvalidQueryException("Got '>' when expected '-', did you forget to add '->' ?", Scanner.Input, null);
                            last = list[list.Count - 1];
                            list[list.Count - 1] = new MatchPath
                            {
                                Alias = last.Alias,
                                IsEdge = last.IsEdge,
                                EdgeType = EdgeType.Right,
                                Recursive = last.Recursive
                            };

                            if (expectNode && Scanner.TryPeek('['))
                            {
                                ThrowExpectedNodeButFoundEdge(last.Alias,last.ToString(), Scanner.Input);
                            }
                            
                            if (Scanner.TryScan('(') == false)
                            {
                                var msg = $"({last.Alias})-> is not allowed, you should use ({last.Alias})-[...] instead.";
                                throw new InvalidQueryException("MATCH operator expected a '(', but didn't get it. " + msg, Scanner.Input, null);
                            }
                            expectNode = true;

                            goto case "(";

                        case "(":
                            var start = Scanner.Position - 1;
                            if (GraphAlias(gq, false, default, out alias) == false)
                                throw new InvalidQueryException("Couldn't get node's alias", Scanner.Input, null);
                            var end = Scanner.Position + 1;

                            if (expectNode == false)
                                ThrowExpectedEdgeButFoundNode(alias,Scanner.Input.Substring(start, end - start),Scanner.Input);

                            if (Scanner.TryScan(')') == false)
                                throw new InvalidQueryException("MATCH operator expected a ')' after reading: " + alias, Scanner.Input, null);                           

                            list.Add(new MatchPath
                            {
                                Alias = alias,
                                EdgeType = list[list.Count - 1].EdgeType
                            });

                            expectNode = false; //the next should be an edge
                            foundLeftArrow = false;
                            break;
                        case "recursive":
                            if (allowRecursive == false)
                                throw new InvalidQueryException("Cannot call 'recusrive' inside another 'recursive', only one level is allowed", Scanner.Input, null);

                            if (expectNode)
                                throw new InvalidQueryException("'recursive' must appear only after a node, not after an edge", Scanner.Input, null);

                            if (foundDash == false)
                                throw new InvalidQueryException("Got 'recursive' when expected '-', recursive must be preceded by a '-'.", Scanner.Input, null);

                            StringSegment recursiveAlias;

                            if (Scanner.TryScan("as"))
                            {
                                if (Scanner.Identifier() == false)
                                    throw new InvalidQueryException("Missing alias for 'recursive' after 'as'", Scanner.Input, null);
                                recursiveAlias = Scanner.Token;
                            }
                            else
                            {
                                recursiveAlias = "__alias" + (++_counter);
                            }

                            gq.RecursiveMatches.Add(recursiveAlias);
                            var options = new List<ValueExpression>();
                            if (Scanner.TryScan('('))
                            {
                                while (Value(out var val))
                                {
                                    switch (val.Value)
                                    {
                                        case ValueTokenType.Parameter:
                                        case ValueTokenType.Long:
                                        case ValueTokenType.String:
                                            options.Add(val);
                                            break;
                                        case ValueTokenType.Double:
                                        case ValueTokenType.True:
                                        case ValueTokenType.False:
                                        case ValueTokenType.Null:
                                            throw new InvalidQueryException("'recursive' options must be an integer or a recursive type (all, shortest, longest)", Scanner.Input, null);
                                        default:
                                            break;
                                    }

                                    if (Scanner.TryScan(',') == false)
                                        break;
                                }

                                if (Scanner.Identifier()) // , longest) , etc.
                                {
                                    options.Add(new ValueExpression(Scanner.Token, ValueTokenType.String));
                                }

                                if (Scanner.TryScan(")") == false)
                                    throw new InvalidQueryException("'recursive' missing closing paranthesis for length specification, but one was expected", Scanner.Input, null);
                            }

                            if (Scanner.TryScan("{") == false)
                                throw new InvalidQueryException("'recursive' must be followed by a '{', but wasn't", Scanner.Input, null);

                            var repeated = new List<MatchPath>();
                            repeated.Add(new MatchPath
                            {
                                Alias = alias,
                                EdgeType = list[list.Count - 1].EdgeType,
                                IsEdge = true
                            });

                            var result = ProcessEdges(gq, out var repeatedPattern, alias, repeated, allowRecursive: false, foundDash: true);
                            if(repeatedPattern is PatternMatchElementExpression pmee)
                            {
                                if (pmee.Reversed)
                                    repeated.RemoveAt(repeated.Count - 1);
                                else
                                    repeated.RemoveAt(0);
                            }
                            else
                            {
                                throw new InvalidQueryException("'recursive' must contain only a single pattern match, but contained " + repeatedPattern, Scanner.Input, null);
                            }

                            if (Scanner.TryScan("}") == false)
                                throw new InvalidQueryException("'recursive' must be closed by '}', but wasn't", Scanner.Input, null);

                            if (repeated.Count == 0)
                            {
                                throw new InvalidQueryException("empty recursive {} block is not allowed ", Scanner.Input, null);
                            }

                            if (repeated.Last().IsEdge)
                            {
                                throw new InvalidQueryException("'recursive' block cannot end with an end and must close with a node ( recursive { [edge]->(node) } )", Scanner.Input, null);
                            }


                            list.Add(new MatchPath
                            {
                                Alias = "recursive",
                                EdgeType = list[list.Count-1].EdgeType,
                                Recursive = new RecursiveMatch
                                {
                                    Alias = recursiveAlias,
                                    Pattern = repeated,
                                    Options = options,
                                    Aliases = new HashSet<StringSegment>(repeated.Select(x => x.Alias), StringSegmentComparer.Ordinal)
                                },
                                IsEdge = true
                            });
                            alias = recursiveAlias;
                            expectNode = false;
                            foundDash = false;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("Unknown edge type: " + found);
                    }

                    foundDash = false;
                }
                else
                {
                    op = FinalProcessingOfMatchExpression(list);

                    return true;
                }
            }
        }

        private void ThrowExpectedEdgeButFoundNode(StringSegment alias, string invalidQueryElement, string query)
        {
            throw new InvalidQueryException(
                $@"Expected the alias '{alias}' to refer an edge, but it refers to an node.{Environment.NewLine}This is likely a mistake in the query as the expression '{invalidQueryElement}' should probably be an edge.", query);
        }

        private void ThrowExpectedNodeButFoundEdge(StringSegment alias, string invalidQueryElement, string query)
        {
            throw new InvalidQueryException(
                $@"Expected the alias '{alias}' to refer a node, but it refers to an edge.{Environment.NewLine}This is likely a mistake in the query as the expression '{invalidQueryElement}' should probably be a node.", query);
        }

        private static QueryExpression FinalProcessingOfMatchExpression(List<MatchPath> list)
        {
            bool hasIncoming = false, hasOutgoing = false;

            for (int i = 0; i < list.Count; i++)
            {
                hasIncoming |= list[i].EdgeType == EdgeType.Left;
                hasOutgoing |= list[i].EdgeType == EdgeType.Right;
            }

            bool reversed = false;
            QueryExpression op;
            if (hasIncoming)
            {
                if (hasOutgoing == false)
                {
                    reversed = true;
                    ReverseIncomingChain(list);
                }
                else
                {
                    return BreakPatternChainToAndClauses(list, out op);
                }
            }

            op = new PatternMatchElementExpression
            {
                Path = list.ToArray(),
                Type = ExpressionType.Pattern,
                Reversed = reversed
            };
            return op;
        }
      
        private static QueryExpression BreakPatternChainToAndClauses(List<MatchPath> matchPaths, out QueryExpression op)
        {
            op = null;
            var clauses = new List<PatternMatchElementExpression>();

            if (matchPaths.Count == 0) //precaution, obviously shouldn't happen
                return null;

            if (matchPaths.Count == 1)
                return new PatternMatchElementExpression
                {
                    Path = matchPaths.ToArray(),
                    Type = ExpressionType.Pattern
                };

            ThrowIfMatchPathIsInvalid(matchPaths);

            //the last path element cannot be a "cross-road"
            while (matchPaths.Count > 1)
            {
                var hasFoundJunction = false;        
                
                //the first two and the last two cannot have different directions, since 
                //minimal query with different directions will look like (node1)-[edge1]->(node2)<-[edge2]-(node3)
                for (var i = 0; i < matchPaths.Count - 2; i++)
                {
                    if (matchPaths[i].EdgeType == matchPaths[i + 1].EdgeType)
                        continue;
                    
                    hasFoundJunction = true;

                    List<MatchPath> subRange = null;
                    switch (matchPaths[i].EdgeType)
                    {
                        case EdgeType.Left when matchPaths[i + 1].EdgeType == EdgeType.Right:
                            subRange = matchPaths.GetRange(0, i + 1);
                            matchPaths.RemoveRange(0, i);
                            break;
                        case EdgeType.Right when matchPaths[i + 1].EdgeType == EdgeType.Left:
                            subRange = matchPaths.GetRange(0, i + 2);
                            matchPaths.RemoveRange(0, i + 1);
                            break;
                    }

                    if (subRange != null)
                    {
                        if (subRange[0].EdgeType == EdgeType.Left)
                            subRange.Reverse();

                        for (var r = 0; r < subRange.Count; r++)
                            subRange[r] = subRange[r].CloneWithDifferentEdgeType(EdgeType.Right);

                        clauses.Add(new PatternMatchElementExpression
                        {
                            Path = subRange.ToArray(),
                            Type = ExpressionType.Pattern
                        });
                    }


                    if (matchPaths[0].EdgeType == EdgeType.Left && matchPaths[1].EdgeType == EdgeType.Right)
                    {
                        matchPaths[0] = matchPaths[0].CloneWithDifferentEdgeType(EdgeType.Right);
                    }

                    break;
                }

                if (hasFoundJunction)
                    continue;
                
                if (matchPaths[0].EdgeType == EdgeType.Left)
                {
                    matchPaths.Reverse();
                    for (var r = 0; r < matchPaths.Count; r++)
                        matchPaths[r] = matchPaths[r].CloneWithDifferentEdgeType(EdgeType.Right);
                }

                clauses.Add(new PatternMatchElementExpression
                {
                    Path = matchPaths.ToArray(),
                    Type = ExpressionType.Pattern
                });
                break;
            }

            op = clauses.Last();
            for (int i = clauses.Count - 2; i >= 0; i--)
            {
                op = new BinaryExpression(op, clauses[i], OperatorType.And);
            }

            return op;
        }

        private static void ThrowIfMatchPathIsInvalid(List<MatchPath> matchPaths)
        {
            var first = matchPaths[0];
            var second = matchPaths[1];
            //since the first element should be a node, we cannot have first and second path element changing directions
            ThrowIfDirectionChanges(matchPaths, first, second, "first");
            if (first.IsEdge)
                throw new InvalidQueryException($"Expected the first query element '{first}' to be a node, but it is an edge. Graph pattern match queries should start from a node element.");

            var last = matchPaths[matchPaths.Count - 1];
            if (last.IsEdge)
                throw new InvalidQueryException($"Expected the last query element '{last}' to be a node, but it is an edge. Graph pattern match queries should end with a node element.");
            
            var beforeLast = matchPaths[matchPaths.Count - 2];

            //the last two path elements should not have different directions because it will have the following pattern:
            // [edge]-> -(node) ==> and this is obviously a meaningless syntax
            ThrowIfDirectionChanges(matchPaths, last, beforeLast, "last");
        }

        private static void ThrowIfDirectionChanges(List<MatchPath> matchPaths, MatchPath elementA, MatchPath elementB, string description)
        {
            if (!elementA.Recursive.HasValue &&
                !elementB.Recursive.HasValue &&
                elementA.EdgeType != elementB.EdgeType)
            {
                var offendingExpression = new PatternMatchElementExpression
                {
                    Path = matchPaths.ToArray(),
                    Type = ExpressionType.Pattern
                };
                throw new InvalidQueryException(
                    $"Pattern match expression that contains '{elementA}' and '{elementB}' is invalid (full expression: '{offendingExpression}'). The {description} two elements (with aliases '{elementB.Alias}' and '{elementA.Alias}') should not change direction.");
            }
        }

        private static void ReverseIncomingChain(List<MatchPath> list)
        {
            // reverse the path so it is always rightward skips
            list.Reverse();
            for (int i = 0; i < list.Count; i++)
            {
                var cur = list[i];
                list[i] = new MatchPath
                {
                    Alias = cur.Alias,
                    EdgeType = EdgeType.Right,
                    IsEdge = cur.IsEdge,
                    Recursive = cur.Recursive
                };
            }
        }

        private (Query Query, StringSegment Allias) With()
        {
            if (Scanner.TryScan('{') == false)
                throw new InvalidQueryException("With keyword should be followed with either 'edges' or '{' ", Scanner.Input, null);

            var query = Parse(recursive: true);

            if (Scanner.TryScan('}') == false)
                throw new InvalidQueryException("With clause contains invalid body", Scanner.Input, null);

            if (Alias(true, out var alias) == false || alias.HasValue == false)
                throw new InvalidQueryException("With clause must contain alias but none was provided", Scanner.Input, null);

            return (query, alias.Value);
        }

        private static Esprima.Ast.Program ValidateScript(string script)
        {
            var javaScriptParser = new JavaScriptParser(script);
            return javaScriptParser.ParseProgram();
        }

        private static void ThrowUnknownQueryType(QueryType queryType)
        {
            throw new ArgumentOutOfRangeException(nameof(queryType), queryType, "Unknown query type");
        }

        internal static string AddLineAndColumnNumberToErrorMessage(Exception e, string msg)
        {
            if (!(e is ParserException pe))
                return msg;

            return $"{msg}{Environment.NewLine}At Line : {pe.LineNumber}, Column : {pe.Column}";
        }

        private List<QueryExpression> IncludeClause()
        {
            List<QueryExpression> includes = new List<QueryExpression>();

            do
            {
                if (Value(out var val))
                {
                    includes.Add(val);
                }
                else if (Field(out var field))
                {
                    QueryExpression expr;
                    if (Scanner.TryScan('('))
                    {
                        if (Method(field, out var method) == false)
                            ThrowParseException("Expected method call in " + field);
                        expr = method;
                    }
                    else
                    {
                        expr = field;
                    }

                    includes.Add(expr);
                }
                else
                {
                    ThrowParseException("Unable to understand include clause expression");
                }
            } while (Scanner.TryScan(","));
            return includes;
        }

        private (StringSegment Name, (string FunctionText, Esprima.Ast.Program Program)) DeclaredFunction()
        {
            // because of how we are processing them, we don't actually care for
            // parsing the function directly. We have implemented a minimal parser
            // here that find the _boundary_ of the function call, and then we hand
            // all of that code directly to the js code. 

            var functionStart = Scanner.Position;

            if (Scanner.TryScan("function") == false)
                ThrowParseException("DECLARE clause found but missing 'function' keyword");

            if (Scanner.Identifier() == false)
                ThrowParseException("DECLARE functions require a name and cannot be anonymous");

            var name = Scanner.Token;

            // this reads the signature of the method: (a,b,c), etc.
            // we are technically allow more complex stuff there that isn't
            // allowed by JS, but that is fine, since the JS parser will break 
            // when it try it, so we are good with false positives here

            if (Scanner.TryScan('(') == false)
                ThrowParseException("Unable to parse function " + name + " signature");

            ReadMethodArguments();

            if (Scanner.FunctionBody() == false)
                ThrowParseException("Unable to get function body for " + name);

            var functionText = Scanner.Input.Substring(functionStart, Scanner.Position - functionStart);
            // validate this function
            try
            {
                var program = ValidateScript(functionText);
                return (name, (functionText, program));
            }
            catch (Exception e)
            {
                var msg = AddLineAndColumnNumberToErrorMessage(e, $"Invalid script inside function {name}");
                throw new InvalidQueryException(msg, Scanner.Input, null, e);
            }
        }

        private List<(QueryExpression Expression, StringSegment? Alias)> GroupBy()
        {
            var fields = new List<(QueryExpression Expression, StringSegment? Alias)>();
            do
            {
                QueryExpression op = null;
                if (Field(out var field))
                {
                    if (Scanner.TryScan('('))
                    {
                        if (Method(field, out var method) == false)
                            ThrowParseException($"Unable to parse method call {field} for GROUP BY");
                        op = method;
                    }
                    else
                    {
                        op = field;
                    }
                }
                else if (Value(out var value))
                {
                    op = value;
                }
                else
                {
                    ThrowParseException("Unable to get field for GROUP BY");
                }

                Alias(false, out var alias);

                fields.Add((op, alias));

                if (Scanner.TryScan(",") == false)
                    break;

            } while (true);

            return fields;
        }

        private List<(QueryExpression Expression, OrderByFieldType OrderingType, bool Ascending)> OrderBy()
        {
            var orderBy = new List<(QueryExpression Expression, OrderByFieldType OrderingType, bool Ascending)>();
            do
            {
                if (Field(out var field) == false)
                    ThrowParseException("Unable to get field for ORDER BY");

                var type = OrderByFieldType.Implicit;

                QueryExpression op;
                if (Scanner.TryScan('('))
                {
                    if (Method(field, out var method) == false)
                        ThrowParseException($"Unable to parse method call {field} for ORDER BY");
                    op = method;
                }
                else
                {
                    op = field;
                }

                if (Scanner.TryScan("AS") && Scanner.TryScan(OrderByAsOptions, out var asMatch))
                {
                    switch (asMatch)
                    {
                        case "string":
                            type = OrderByFieldType.String;
                            break;
                        case "long":
                            type = OrderByFieldType.Long;
                            break;
                        case "double":
                            type = OrderByFieldType.Double;
                            break;
                        case "alphaNumeric":
                            type = OrderByFieldType.AlphaNumeric;
                            break;
                    }
                }

                var asc = true;

                if (Scanner.TryScan(OrderByOptions, out var match))
                {
                    if (match == "DESC" || match == "DESCENDING")
                        asc = false;
                }

                orderBy.Add((op, type, asc));

                if (Scanner.TryScan(",") == false)
                    break;
            } while (true);
            return orderBy;
        }

        private List<(QueryExpression, StringSegment?)> SelectClause(string clause, Query query)
        {
            query.IsDistinct = Scanner.TryScan("DISTINCT");

            if (Scanner.TryScan("*"))
                return null;

            var functionStart = Scanner.Position;
            if (Scanner.FunctionBody())
            {
                query.SelectFunctionBody.FunctionText = Scanner.Input.Substring(functionStart, Scanner.Position - functionStart);
                return new List<(QueryExpression, StringSegment?)>();
            }

            return SelectClauseExpressions(clause, true);
        }

        private List<(QueryExpression, StringSegment?)> SelectClauseExpressions(string clause, bool aliasAsRequired)
        {
            var select = new List<(QueryExpression Expr, StringSegment? Alias)>();

            do
            {
                QueryExpression expr;
                if (Field(out var field))
                {
                    if (Scanner.TryScan('('))
                    {
                        if (Method(field, out var method) == false)
                            ThrowParseException("Expected method call in " + clause);
                        expr = method;
                    }
                    else
                    {
                        expr = field;
                    }
                }
                else if (Value(out var v))
                {
                    expr = v;
                }
                else
                {
                    ThrowParseException("Unable to get field for " + clause);
                    return null; // never called
                }

                if (Alias(aliasAsRequired, out var alias) == false && expr is ValueExpression ve)
                {
                    alias = ve.Token;
                }

                select.Add((expr, alias));

                if (Scanner.TryScan(",") == false)
                    break;
            } while (true);
            return select;
        }

        private bool TryParseFromClause(out FromClause fromClause, out string message)
        {
            fromClause = default;
            if (Scanner.TryScan("FROM") == false)
            {
                message = "Expected FROM clause";
                return false;
            }

            return TryParseExpressionAfterFromKeyword(out fromClause, out message);
        }

        private bool TryParseExpressionAfterFromKeyword(out FromClause fromClause, out string message)
        {
            FieldExpression field;
            QueryExpression filter = null;
            fromClause = default;
            message = string.Empty;
            var index = false;
            if (Scanner.TryScan("INDEX"))
            {
                if (Field(out field) == false)
                {
                    message = "Invalid syntax. Expected 'INDEX source' expression.";
                    return false;
                }

                index = true;
            }
            else
            {
                if (Field(out field) == false)
                {
                    message = "Unable to parse identifier in FROM clause";
                    return false;
                }

                if (Scanner.TryScan('(')) // Collection ( filter )
                {
                    if (Expression(out filter) == false)
                    {
                        message = "Expected filter in filtered FROM clause";
                        return false;
                    }

                    if (Scanner.TryScan(')') == false)
                    {
                        message = "Expected closing parenthesis in filtered FORM clause after filter";
                        return false;
                    }
                }
            }


            Alias(false, out var alias);

            fromClause = new FromClause
            {
                From = field,
                Alias = alias,
                Filter = filter,
                Index = index
            };


            return true;
        }

        private FromClause FromClause()
        {
            if(!TryParseFromClause(out var fromClause, out var msg))
                ThrowParseException(msg);

            return fromClause;
        }

        private static readonly string[] AliasKeywords =
        {
            "AS",
            "SELECT",
            "WHERE",
            "LOAD",
            "GROUP",
            "ORDER",
            "INCLUDE",
            "UPDATE",
            "OFFSET",
            "LIMIT"
        };

        private bool Alias(bool aliasAsRequired, out StringSegment? alias)
        {
            bool required = false;
            if (Scanner.TryScan(AliasKeywords, out var match))
            {
                required = true;
                if (match != "AS")
                {
                    // found a keyword
                    Scanner.GoBack(match.Length);
                    alias = null;
                    return false;
                }
            }
            if (aliasAsRequired && required == false)
            {
                alias = null;
                return false;
            }

            if (Field(out var token))
            {
                alias = token.FieldValue;
                return true;
            }

            if (required)
                ThrowParseException("Expected field alias after AS in SELECT");

            alias = null;
            return false;
        }

        internal bool Parameter(out StringSegment p)
        {
            if (Scanner.TryScan('$') == false)
            {
                p = default;
                return false;
            }

            if (Scanner.Identifier(false) == false)
                ThrowParseException("Expected parameter name");

            p = Scanner.Token;
            return true;
        }

        internal bool Expression(out QueryExpression op)
        {
            if (++_depth > 128)
                ThrowQueryException("Query is too complex, over 128 nested clauses are not allowed");
            if (Scanner.Position != _statePos)
            {
                _statePos = Scanner.Position;
                _state = NextTokenOptions.Parenthesis;
            }
            var result = Binary(out op);
            _depth--;
            return result;
        }

        private bool Binary(out QueryExpression op)
        {
            switch (_state)
            {
                case NextTokenOptions.Parenthesis:
                    if (Parenthesis(out op) == false)
                        return false;
                    break;
                case NextTokenOptions.BinaryOp:
                    _state = NextTokenOptions.Parenthesis;
                    if (Operator(OperatorField.Required, out op) == false)
                        return false;
                    break;
                default:
                    op = null;
                    return false;
            }


            if (Scanner.TryScan(BinaryOperators, out var found) == false)
                return true; // found simple

            var negate = Scanner.TryScan("NOT");
            var type = found == "OR"
                ? OperatorType.Or
                : OperatorType.And;

            _state = NextTokenOptions.Parenthesis;

            var parenthesis = Scanner.TryPeek('(');

            if (Binary(out var right) == false)
                ThrowParseException($"Failed to find second part of {type} expression");

            return TrySimplifyBinaryExpression(right, type, negate, parenthesis, ref op);
        }

        private bool TrySimplifyBinaryExpression(
            QueryExpression right,
            OperatorType type,
            bool negate,
            bool parenthesis,
            ref QueryExpression op)
        {
            if (parenthesis == false)
            {
                if (negate)
                {
                    right = NegateExpressionWithoutParenthesis(right);
                }

                // if the other arg isn't parenthesis, use operator precedence rules
                // to re-write the query
                switch (type)
                {
                    case OperatorType.And:
                        if (right is BinaryExpression rightOp)
                        {
                            switch (rightOp.Operator)
                            {
                                case OperatorType.Or:
                                case OperatorType.And:

                                    rightOp.Left = new BinaryExpression(op, rightOp.Left, type);
                                    op = right;
                                    return true;
                            }
                        }
                        break;
                }
            }
            else if (negate)
            {
                right = new NegatedExpression(right);
            }

            op = new BinaryExpression(op, right, type)
            {
                Parenthesis = parenthesis
            };

            return true;
        }

        private QueryExpression NegateExpressionWithoutParenthesis(QueryExpression expr)
        {
            bool ShouldRecurse(BinaryExpression e)
            {
                if (e.Parenthesis)
                    return false;

                return e.Operator == OperatorType.And ||
                       e.Operator == OperatorType.Or;
            }

            if (expr is BinaryExpression be && ShouldRecurse(be))
            {
                var result = be;

                while (be.Left is BinaryExpression nested && ShouldRecurse(nested))
                {
                    be = nested;
                }
                be.Left = new NegatedExpression(be.Left);

                return result;
            }
            return new NegatedExpression(expr);
        }

        private bool Parenthesis(out QueryExpression op)
        {
            if (Scanner.TryScan('(') == false)
            {
                _state = NextTokenOptions.BinaryOp;
                return Binary(out op);
            }

            if (Expression(out op) == false)
                return false;

            if (Scanner.TryScan(')') == false)
                ThrowParseException("Unmatched parenthesis, expected ')'");
            return true;
        }

        private enum OperatorField
        {
            Required,
            Optional,
            Desired
        }

        private bool Operator(OperatorField fieldOption, out QueryExpression op)
        {
            OperatorType type;
            FieldExpression field = null;

            if (Scanner.TryScan("true"))
            {
                op = new TrueExpression();
                return true;
            }

            if (fieldOption != OperatorField.Optional && Field(out field) == false)
            {
                op = null;
                return false;
            }

            if (Scanner.TryScan(OperatorStartMatches, out var match) == false)
            {
                if (fieldOption != OperatorField.Required)
                {
                    op = field;
                    return fieldOption == OperatorField.Desired;
                }
                ThrowParseException("Invalid operator expected any of (In, Between, =, <, >, <=, >=, !=)");
            }


            switch (match)
            {
                case "<":
                    type = OperatorType.LessThan;
                    break;
                case ">":
                    type = OperatorType.GreaterThan;
                    break;
                case "<=":
                    type = OperatorType.LessThanEqual;
                    break;
                case ">=":
                    type = OperatorType.GreaterThanEqual;
                    break;
                case "=":
                case "==":
                    type = OperatorType.Equal;
                    break;
                case "!=":
                case "<>":
                    type = OperatorType.NotEqual;
                    break;
                case "BETWEEN":
                    if (Value(out var fst) == false)
                        ThrowParseException("parsing Between, expected value (1st)");
                    if (Scanner.TryScan("AND") == false)
                        ThrowParseException("parsing Between, expected AND");
                    if (Value(out var snd) == false)
                        ThrowParseException("parsing Between, expected value (2nd)");

                    if (fst.Type != snd.Type)
                        ThrowQueryException(
                            $"Invalid Between expression, values must have the same type but got {fst.Type} and {snd.Type}");

                    op = new BetweenExpression(field, fst, snd);
                    return true;
                case "IN":
                case "ALL IN":
                    if (Scanner.TryScan('(') == false)
                        ThrowParseException("parsing In, expected '('");

                    var list = new List<QueryExpression>();
                    do
                    {
                        if (Scanner.TryScan(')'))
                            break;

                        if (list.Count != 0)
                            if (Scanner.TryScan(',') == false)
                                ThrowParseException("parsing In expression, expected ','");

                        if (Value(out var inVal) == false)
                            ThrowParseException("parsing In, expected a value");

                        if (list.Count > 0)
                            if (list[0].Type != inVal.Type)
                                ThrowQueryException(
                                    $"Invalid In expression, all values must have the same type, expected {list[0].Type} but got {inVal.Type}");
                        list.Add(inVal);
                    } while (true);

                    op = new InExpression(field, list, match == "ALL IN");

                    return true;
                case "(":
                    var isMethod = Method(field, out var method);
                    op = method;

                    if (isMethod && Operator(OperatorField.Optional, out var methodOperator))
                    {
                        if (methodOperator is BinaryExpression be)
                        {
                            be.Left = method;
                            op = be;
                            return true;
                        }

                        if (methodOperator is InExpression ie)
                        {
                            ie.Source = method;
                            op = ie;
                            return true;
                        }
                        if (methodOperator is BetweenExpression between)
                        {
                            between.Source = method;
                            op = between;
                            return true;
                        }
                        if (methodOperator is MethodExpression me)
                        {
                            op = me;
                            return true;
                        }
                        ThrowParseException("Unexpected operator after method call: " + methodOperator);
                    }

                    return isMethod;
                default:
                    op = null;
                    return false;
            }

            if (Value(out var val))
            {
                op = new BinaryExpression(field, val, type);
                return true;
            }

            if (Operator(OperatorField.Desired, out var op2))
            {
                op = new BinaryExpression(field, op2, type);
                return true;
            }

            op = null;
            return false;
        }

        private bool Method(FieldExpression field, out MethodExpression op)
        {
            var args = ReadMethodArguments();

            op = new MethodExpression(field.FieldValue, args);
            return true;
        }

        private List<QueryExpression> ReadMethodArguments()
        {
            var args = new List<QueryExpression>();
            do
            {
                if (Scanner.TryScan(')'))
                    break;

                if (args.Count != 0)
                    if (Scanner.TryScan(',') == false)
                        ThrowParseException("parsing method expression, expected ','");

                var maybeExpression = false;
                if (Value(out var argVal))
                {
                    if (Scanner.TryPeek(',') == false && Scanner.TryPeek(')') == false)
                    {
                        // this is not a simple field ref, let's parse as full expression

                        Scanner.Reset(argVal.Token.Offset - 1); // if this was a value then it had to be in ''
                        maybeExpression = true;
                    }
                    else
                    {
                        args.Add(argVal);
                        continue;
                    }
                }

                if (maybeExpression == false && Field(out var fieldRef))
                {
                    if (Scanner.TryPeek(',') == false && Scanner.TryPeek(')') == false)
                    {
                        // this is not a simple field ref, let's parse as full expression

                        Scanner.Reset(fieldRef.Compound[0].Offset);
                    }
                    else
                    {
                        args.Add(fieldRef);
                        continue;
                    }
                }

                if (Expression(out var expr))
                    args.Add(expr);
                else
                    ThrowParseException("parsing method, expected an argument");
            } while (true);
            return args;
        }

        private void ThrowParseException(string msg)
        {
            var sb = new StringBuilder()
                .Append(Scanner.Line)
                .Append(":")
                .Append(Scanner.Column)
                .Append(" ")
                .Append(msg)
                .Append(" but got");

            if (Scanner.NextToken())
                sb.Append(": ")
                    .Append(Scanner.CurrentToken);
            else
                sb.Append(" to the end of the query");


            sb.AppendLine();
            sb.AppendLine("Query: ");
            sb.Append(Scanner.Input);

            throw new ParseException(sb.ToString());
        }

        private void ThrowQueryException(string msg)
        {
            var sb = new StringBuilder()
                .Append(Scanner.Column)
                .Append(":")
                .Append(Scanner.Line)
                .Append(" ")
                .Append(msg);

            throw new ParseException(sb.ToString());
        }

        private bool Value(out ValueExpression val)
        {
            var numberToken = Scanner.TryNumber();
            if (numberToken != null)
            {
                val = new ValueExpression(Scanner.Token,
                    numberToken.Value == NumberToken.Long ? ValueTokenType.Long : ValueTokenType.Double
                );
                return true;
            }
            if (Scanner.String(out var token))
            {
                val = new ValueExpression(
                    token,
                    ValueTokenType.String
                );
                return true;
            }
            if (Scanner.TryScan(StaticValues, out var match))
            {
                ValueTokenType type;
                switch (match)
                {
                    case "true":
                        type = ValueTokenType.True;
                        break;
                    case "false":
                        type = ValueTokenType.False;
                        break;
                    case "null":
                        type = ValueTokenType.Null;
                        break;
                    default:
                        type = ValueTokenType.String;
                        break;
                }

                val = new ValueExpression(
                    Scanner.Token,
                    type);
                return true;
            }

            if (Parameter(out _))
            {
                val = new ValueExpression(
                    Scanner.Token,
                    ValueTokenType.Parameter
                );
                return true;
            }
            val = null;
            return false;
        }

        internal bool Field(out FieldExpression token)
        {
            var part = 0;

            var parts = new List<StringSegment>(1);
            bool quoted = false;
            while (true)
            {
                if (Scanner.Identifier(beginning: part++ == 0) == false)
                {
                    if (Scanner.String(out var str))
                    {
                        if (part == 1)
                            quoted = true;
                        parts.Add(str);
                    }
                    else
                    {
                        token = null;
                        return false;
                    }
                }
                else
                {
                    parts.Add(Scanner.Token);
                }
                if (part == 1)
                {
                    // need to ensure that this isn't a keyword
                    if (Scanner.CurrentTokenMatchesAnyOf(AliasKeywords))
                    {
                        Scanner.GoBack();
                        token = null;
                        return false;
                    }
                }

                bool? hasNextPart = null;

                while (Scanner.TryScan('['))
                {
                    switch (Scanner.TryNumber())
                    {
                        case NumberToken.Long:
                            if (Scanner.TryScan(']') == false)
                                ThrowParseException("Expected to find closing ]");
                            parts.Add(Scanner.Token);
                            break;

                        case null:
                            if (Scanner.TryScan(']') == false)
                                ThrowParseException("Expected to find closing ]");
                            parts.Add("[]");

                            break;
                        case NumberToken.Double:
                            ThrowParseException("Array indexer must be integer, but got double");
                            break;
                    }

                    hasNextPart = Scanner.TryScan('.');
                }

                if (hasNextPart == true)
                    continue;

                if (Scanner.TryScan('.') == false)
                    break;
            }


            token = new FieldExpression(parts)
            {
                IsQuoted = quoted
            };

            return true;
        }

        private enum NextTokenOptions
        {
            Parenthesis,
            BinaryOp
        }

        public class ParseException : Exception
        {
            public ParseException(string msg) : base(msg)
            {
            }
        }
    }
}
