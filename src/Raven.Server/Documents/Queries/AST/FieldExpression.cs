using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Primitives;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class FieldExpression : QueryExpression
    {
        public List<StringSegment> Compound;

        public bool IsQuoted;

        private string _field;
        private string _fieldWithoutAlias;

        public FieldExpression(List<StringSegment> path)
        {
            Compound = path;
            Type = ExpressionType.Field;
        }

        public bool HasCollectionOperator
        {
            get
            {
                for (int i = 1; i < Compound.Count; i++)
                {
                    if (Compound[i] == "[]")
                        return true;
                }

                return false;
            }
        }

        public string FieldValue => GetFieldValue(false);

        private string GetFieldValue(bool ignoreArrayQualifier)
        {
            if (Compound.Count == 1)
                return Compound[0].Value;
            return _field ?? (_field = JoinCompoundFragments(0));
        }


        private string JoinCompoundFragments(int start)
        {
            var sb = new StringBuilder();
            for (int i = start; i < Compound.Count; i++)
            {
                if(i == start && Compound[i].Value == "[]") //field name starting from '[]' makes no sense
                    continue;
                sb.Append(Compound[i].Value);
                if (i + 1 < Compound.Count && Compound[i + 1] != "[]")
                {
                    sb.Append(".");
                }
            }
            return sb.ToString();
        }

        public string FieldValueWithoutAlias => 
            _fieldWithoutAlias ?? (_fieldWithoutAlias = JoinCompoundFragments(1));

        public override string ToString()
        {
            return "<Field>: " + string.Join(".", Compound);
        }

        public override string GetText(IndexQueryServerSide parent)
        {
            return string.IsNullOrEmpty(FieldValueWithoutAlias) 
                ? FieldValue 
                : FieldValueWithoutAlias;
        }

        public override bool Equals(QueryExpression other)
        {
            if (!(other is FieldExpression of))
                return false;


            return string.IsNullOrEmpty(FieldValueWithoutAlias) ?
                FieldValue == of.FieldValue :
                FieldValueWithoutAlias == of.FieldValueWithoutAlias;

        }
    }
}
