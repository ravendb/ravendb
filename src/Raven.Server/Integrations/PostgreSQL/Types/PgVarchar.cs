using System;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL.Types
{
    public class PgVarchar : PgType
    {
        public static readonly PgVarchar Default = new();
        public override int Oid => PgTypeOIDs.Varchar;
        public override short Size => -1;
        public override int TypeModifier => -1;

        public override ReadOnlyMemory<byte> ToBytes(object value, PgFormat formatCode)
        {
            var str = value.ToString() ?? string.Empty;
            if (TypeModifier != -1 && str.Length > TypeModifier)
            {
                throw new PgErrorException(PgErrorCodes.StringDataRightTruncation,
                    $"Value too long ({str.Length}) for type character varying({TypeModifier})");
            }

            return Utf8GetBytes(value);
        }

        public override object FromBytes(byte[] buffer, PgFormat formatCode)
        {
            var str = Utf8GetString(buffer);
            return FromString(str);
        }

        public override object FromString(string value)
        {
            if (TypeModifier != -1 && value.Length > TypeModifier)
            {
                throw new PgErrorException(PgErrorCodes.StringDataRightTruncation,
                    $"Converted value too long ({value.Length}) for type character varying({TypeModifier})");
            }

            return value;
        }
    }
}
