/*
    Copyright (c) 2002-2021, Npgsql

    Permission to use, copy, modify, and distribute this software and its
    documentation for any purpose, without fee, and without a written agreement
    is hereby granted, provided that the above copyright notice and this
    paragraph and the following two paragraphs appear in all copies.

    IN NO EVENT SHALL NPGSQL BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT,
    SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS,
    ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
    Npgsql HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

    NPGSQL SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED
    TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
    PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND Npgsql
    HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS,
    OR MODIFICATIONS.
 */

namespace Raven.Server.Integrations.PostgreSQL.Types
{
    /// <summary>
    /// Holds well-known, built-in PostgreSQL type OIDs.
    /// </summary>
    public static class PgTypeOIDs
    {
        // Numeric
        public const int Int8 = 20;
        public const int Float8 = 701;
        public const int Int4 = 23;
        public const int Numeric = 1700;
        public const int Float4 = 700;
        public const int Int2 = 21;
        public const int Money = 790;

        // Boolean
        public const int Bool = 16;

        // Geometric
        public const int Box = 603;
        public const int Circle = 718;
        public const int Line = 628;
        public const int LSeg = 601;
        public const int Path = 602;
        public const int Point = 600;
        public const int Polygon = 604;

        // Character
        public const int BPChar = 1042;
        public const int Text = 25;
        public const int Varchar = 1043;
        public const int Name = 19;
        public const int Char = 18;

        // Binary data
        public const int Bytea = 17;

        // Date/Time
        public const int Date = 1082;
        public const int Time = 1083; // Time without timezone
        public const int Timestamp = 1114; // Timestamp without timezone
        public const int TimestampTz = 1184; // Timestamp with timezone
        public const int Interval = 1186;
        public const int TimeTz = 1266; // Time with timezone
        //public const int Abstime = 702;

        // Network address
        public const int Inet = 869;
        public const int Cidr = 650;
        public const int Macaddr = 829;
        public const int Macaddr8 = 774;

        // Bit string
        public const int Bit = 1560;
        public const int Varbit = 1562;

        // Text search
        public const int TsVector = 3614;
        public const int TsQuery = 3615;
        public const int Regconfig = 3734;

        // UUID
        public const int Uuid = 2950;

        // XML
        public const int Xml = 142;

        // JSON
        public const int Json = 114;
        public const int Jsonb = 3802;
        public const int JsonPath = 4072;

        // public
        public const int Refcursor = 1790;
        public const int Oidvector = 30;
        public const int Int2vector = 22;
        public const int Oid = 26;
        public const int Xid = 28;
        public const int Cid = 29;
        public const int Regtype = 2206;
        public const int Tid = 27;

        // Special
        public const int Unknown = 705;
    }
}
