using System.Collections.Generic;
using System.Text;
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

        public string FieldValue
        {
            get
            {
                if (Compound.Count == 1)
                    return Compound[0];
                if (_field == null)
                    _field = JoinCompoundFragements(0);
                return _field;
            }
        }

        private string JoinCompoundFragements(int start)
        {
            var sb = new StringBuilder();
            for (int i = start; i < Compound.Count; i++)
            {
                sb.Append(Compound[i]);
                if (i + 1 < Compound.Count && Compound[i + 1] != "[]")
                {
                    sb.Append(".");
                }
            }
            return sb.ToString();
        }


        public string FieldValueWithoutAlias
        {
            get
            {
                if (_fieldWithoutAlias == null)
                    _fieldWithoutAlias = JoinCompoundFragements(1);
                return _fieldWithoutAlias;
            }
        }

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
    }
}
