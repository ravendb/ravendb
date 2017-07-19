using System;
using System.Collections.Generic;
using Lucene.Net.Search;
using Raven.Client.Documents.Queries;

namespace Raven.Server.Documents.Queries.Parser.Lucene
{
    public class OperatorLuceneASTNode : LuceneASTNodeBase
    {
        public OperatorLuceneASTNode(LuceneASTNodeBase leftNode, LuceneASTNodeBase rightNode, Operator op, bool isDefaultOperatorAnd)
        {
            var rightHandBooleanNode = rightNode as OperatorLuceneASTNode;
            //This basically say that if we have a nested boolean query and the child boolean is not an OR operation do nothing special.
            if (rightHandBooleanNode == null ||
                rightHandBooleanNode.Op == Operator.AND ||
                rightHandBooleanNode.Op == Operator.INTERSECT ||
                (rightHandBooleanNode.Op == Operator.Implicit && isDefaultOperatorAnd))
            {
                LeftNode = leftNode;
                RightNode = rightNode;
                Op = op;
            }
            //if we are in this case we are a boolean query with a child who is a boolean query with an OR operator .
            //we shall roll the nodes so (A) and (B or C) will become (A and b) or (C).
            else
            {
                LeftNode = new OperatorLuceneASTNode(leftNode, rightHandBooleanNode.LeftNode, op, isDefaultOperatorAnd);
                RightNode = rightHandBooleanNode.RightNode;
                Op = Operator.OR;
                //this should not be used but if it does a NRE exception will be thrown and it would be easy to locate it.
                rightHandBooleanNode.RightNode = null;
                rightHandBooleanNode.LeftNode = null;
            }
        }
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get
            {
                yield return LeftNode;
                if (RightNode != null) yield return RightNode;
            }
        }
        public override global::Lucene.Net.Search.Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            var query = new BooleanQuery();
            switch (Op)
            {
                case Operator.AND:
                    LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(LeftNode, Occur.MUST));
                    RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(RightNode, Occur.MUST));
                    break;
                case Operator.OR:
                    LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(LeftNode, Occur.SHOULD));
                    RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(RightNode, Occur.SHOULD));
                    break;
                case Operator.Implicit:
                    switch (configuration.DefaultOperator)
                    {
                        case QueryOperator.Or:
                            LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(LeftNode, Occur.SHOULD));
                            RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(RightNode, Occur.SHOULD));
                            break;
                        case QueryOperator.And:
                            LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(LeftNode, Occur.MUST));
                            RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(RightNode, Occur.MUST));
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("defaultOperator");
                    }
                    break;
                case Operator.INTERSECT:
                    LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(LeftNode, Occur.MUST));
                    RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(RightNode, Occur.MUST));
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            return query;
        }

        private Occur PrefixToOccurrence(LuceneASTNodeBase node, Occur defaultOccurrence)
        {
            switch (node.Prefix)
            {
                case PrefixOperator.None:
                    return defaultOccurrence;
                case PrefixOperator.Plus:
                    return Occur.MUST;
                case PrefixOperator.Minus:
                    return Occur.MUST_NOT;
                default:
                    throw new ArgumentOutOfRangeException("Unknown query prefix " + node.Prefix);
            }
        }

        public override void AddQueryToBooleanQuery(BooleanQuery query, LuceneASTQueryConfiguration configuration, Occur o)
        {
            switch (Op)
            {
                case Operator.AND:
                    LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(LeftNode, Occur.MUST));
                    RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(RightNode, Occur.MUST));
                    break;
                case Operator.OR:
                    LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(LeftNode, Occur.SHOULD));
                    RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(RightNode, Occur.SHOULD));
                    break;
                case Operator.Implicit:
                    switch (configuration.DefaultOperator)
                    {
                        case QueryOperator.Or:
                            LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(LeftNode, Occur.SHOULD));
                            RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(RightNode, Occur.SHOULD));
                            break;
                        case QueryOperator.And:
                            LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(LeftNode, Occur.MUST));
                            RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(RightNode, Occur.MUST));
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("defaultOperator");
                    }
                    break;
                case Operator.INTERSECT:
                    LeftNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(LeftNode, Occur.MUST));
                    RightNode.AddQueryToBooleanQuery(query, configuration, PrefixToOccurrence(RightNode, Occur.MUST));
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public enum Operator
        {
            AND,
            OR,
            Implicit,
            INTERSECT
        }

        public LuceneASTNodeBase RightNode { get; set; }

        public LuceneASTNodeBase LeftNode { get; set; }

        public Operator Op { get; set; }

        public override string ToString()
        {
            if (Op == Operator.Implicit)
            {
                return LeftNode + " " + RightNode;
            }
            return LeftNode + " " + Op + " " + RightNode;
        }
    }
}