using System.Collections.Generic;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.Npgsql
{
    /// <summary>
    /// Support for Npgsql is from version 3.2.3 onwards.
    /// </summary>
    public static class NpgsqlConfig
    {
        // 5.0.0 - current
        public static readonly string VersionQuery = "select version()";
        public static readonly PgTable VersionResponse = CsvToPg.Convert(
            @"version_query.csv",
            new Dictionary<string, PgColumn>
            {
                {"version", new PgColumn("version", 0, PgText.Default, PgFormat.Text)}
            });

        public static readonly string VersionCurrentSettingQuery = "select version();select current_setting('max_index_keys')";
        public static readonly PgTable VersionCurrentSettingResponse = CsvToPg.Convert(
            @"current_setting_query.csv",
            new Dictionary<string, PgColumn> 
                {
                    {"version", new PgColumn("version", 0,PgText.Default, PgFormat.Text)},
                    {"current_setting", new PgColumn("current_setting",1,PgText.Default, PgFormat.Text)}
                });

        public static readonly string CurrentSettingQuery = "select current_setting('max_index_keys')";
        public static readonly PgTable CurrentSettingResponse = CsvToPg.Convert(
            @"current_setting_query.csv",
            new Dictionary<string, PgColumn> 
                {
                    {"current_setting", new PgColumn("current_setting",0,PgText.Default, PgFormat.Text)}
                }
        );

        
        // 5.0.0 - current
        public static readonly string Npgsql5TypesQuery = "SELECT ns.nspname, typ_and_elem_type.*,\n   CASE\n       WHEN typtype IN ('b', 'e', 'p') THEN 0           -- First base types, enums, pseudo-types\n       WHEN typtype = 'r' THEN 1                        -- Ranges after\n       WHEN typtype = 'c' THEN 2                        -- Composites after\n       WHEN typtype = 'd' AND elemtyptype <> 'a' THEN 3 -- Domains over non-arrays after\n       WHEN typtype = 'a' THEN 4                        -- Arrays before\n       WHEN typtype = 'd' AND elemtyptype = 'a' THEN 5  -- Domains over arrays last\n    END AS ord\nFROM (\n    -- Arrays have typtype=b - this subquery identifies them by their typreceive and converts their typtype to a\n    -- We first do this for the type (innerest-most subquery), and then for its element type\n    -- This also returns the array element, range subtype and domain base type as elemtypoid\n    SELECT\n        typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.typrelid, typ.typnotnull, typ.relkind,\n        elemtyp.oid AS elemtypoid, elemtyp.typname AS elemtypname, elemcls.relkind AS elemrelkind,\n        CASE WHEN elemproc.proname='array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype\n    FROM (\n        SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind, typelem AS elemoid,\n            CASE WHEN proc.proname='array_recv' THEN 'a' ELSE typ.typtype END AS typtype,\n            CASE\n                WHEN proc.proname='array_recv' THEN typ.typelem\n                WHEN typ.typtype='r' THEN rngsubtype\n                WHEN typ.typtype='d' THEN typ.typbasetype\n            END AS elemtypoid\n        FROM pg_type AS typ\n        LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid)\n        LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive\n        LEFT JOIN pg_range ON (pg_range.rngtypid = typ.oid)\n    ) AS typ\n    LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid\n    LEFT JOIN pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid)\n    LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive\n) AS typ_and_elem_type\nJOIN pg_namespace AS ns ON (ns.oid = typnamespace)\nWHERE\n    typtype IN ('b', 'r', 'e', 'd') OR -- Base, range, enum, domain\n    (typtype = 'c' AND relkind='c') OR -- User-defined free-standing composites (not table composites) by default\n    (typtype = 'p' AND typname IN ('record', 'void')) OR -- Some special supported pseudo-types\n    (typtype = 'a' AND (  -- Array of...\n        elemtyptype IN ('b', 'r', 'e', 'd') OR -- Array of base, range, enum, domain\n        (elemtyptype = 'p' AND elemtypname IN ('record', 'void')) OR -- Arrays of special supported pseudo-types\n        (elemtyptype = 'c' AND elemrelkind='c') -- Array of user-defined free-standing composites (not table composites) by default\n    ))\nORDER BY ord";
        public static readonly PgTable Npgsql5TypesResponse = CsvToPg.Convert(
            @"npgsql_types_5.csv",
            new Dictionary<string, PgColumn>
            {
                {"nspname", new PgColumn("nspname", 0, PgName.Default, PgFormat.Text)},
                {"oid", new PgColumn("oid", 1, PgOid.Default, PgFormat.Text)},
                {"typnamespace", new PgColumn("typnamespace", 2, PgOid.Default, PgFormat.Text)},
                {"typname", new PgColumn("typname", 3, PgName.Default, PgFormat.Text)},
                {"typtype", new PgColumn("typtype", 4, PgChar.Default, PgFormat.Text, 1)},
                {"typrelid", new PgColumn("typrelid", 5, PgOid.Default, PgFormat.Text)},
                {"typnotnull", new PgColumn("typnotnull", 6, PgBool.Default, PgFormat.Text)},
                {"relkind", new PgColumn("relkind", 7, PgChar.Default, PgFormat.Text, 1)},
                {"elemtypoid", new PgColumn("elemtypoid", 8, PgOid.Default, PgFormat.Text)},
                {"elemtypname", new PgColumn("elemtypname", 9, PgName.Default, PgFormat.Text)},
                {"elemrelkind", new PgColumn("elemrelkind", 10, PgChar.Default, PgFormat.Text, 1)},
                {"elemtyptype", new PgColumn("elemtyptype", 11, PgChar.Default, PgFormat.Text, 1)},
                {"ord", new PgColumn("ord", 12, PgInt4.Default, PgFormat.Text)},
            });

        // 4.1.3 - current
        public static readonly string Npgsql5EnumTypesQuery = "-- Load enum fields\nSELECT pg_type.oid, enumlabel\nFROM pg_enum\nJOIN pg_type ON pg_type.oid=enumtypid\nORDER BY oid, enumsortorder";
        public static readonly PgTable Npgsql5EnumTypesResponse = new()
        {
            Columns = new List<PgColumn>
            {
                new PgColumn("oid", 0, PgOid.Default, PgFormat.Text),
                new PgColumn("enumlabel", 1, PgName.Default, PgFormat.Text),
            }
        };

        // 4.1.3 - current
        public static readonly string Npgsql5CompositeTypesQuery = "-- Load field definitions for (free-standing) composite types\nSELECT typ.oid, att.attname, att.atttypid\nFROM pg_type AS typ\nJOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)\nJOIN pg_class AS cls ON (cls.oid = typ.typrelid)\nJOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)\nWHERE\n  (typ.typtype = 'c' AND cls.relkind='c') AND\n  attnum > 0 AND     -- Don't load system attributes\n  NOT attisdropped\nORDER BY typ.oid, att.attnum";
        public static readonly PgTable Npgsql5CompositeTypesResponse = new()
        {
            Columns = new List<PgColumn>
            {
                new PgColumn("oid", 0, PgOid.Default, PgFormat.Text),
                new PgColumn("attname", 1, PgName.Default, PgFormat.Text),
                new PgColumn("atttypid", 2, PgOid.Default, PgFormat.Text),
            }
        };

        // 4.0.0 - 4.1.1
        public static readonly string EnumTypesQuery = "/*** Load enum fields ***/\nSELECT pg_type.oid, enumlabel\nFROM pg_enum\nJOIN pg_type ON pg_type.oid=enumtypid\nORDER BY oid, enumsortorder";
        public static readonly PgTable EnumTypesResponse = new()
        {
            Columns = new List<PgColumn>
            {
                new PgColumn("oid", 0, PgOid.Default, PgFormat.Text),
                new PgColumn("enumlabel", 1, PgName.Default, PgFormat.Text),
            }
        };

        // 4.0.4 - 4.1.1
        public static readonly string CompositeTypesQuery = "/*** Load field definitions for (free-standing) composite types ***/\nSELECT typ.oid, att.attname, att.atttypid\nFROM pg_type AS typ\nJOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)\nJOIN pg_class AS cls ON (cls.oid = typ.typrelid)\nJOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)\nWHERE\n  (typ.typtype = 'c' AND cls.relkind='c') AND\n  attnum > 0 AND     /* Don't load system attributes */\n  NOT attisdropped\nORDER BY typ.oid, att.attnum";
        public static readonly PgTable CompositeTypesResponse = new()
        {
            Columns = new List<PgColumn>
            {
                new PgColumn("oid", 0, PgOid.Default, PgFormat.Text),
                new PgColumn("attname", 1, PgName.Default, PgFormat.Text),
                new PgColumn("atttypid", 2, PgOid.Default, PgFormat.Text),
            }
        };

        // 4.1.3 - 4.1.9
        public static readonly string Npgsql4TypesQuery = "\nSELECT ns.nspname, typ_and_elem_type.*,\n   CASE\n       WHEN typtype IN ('b', 'e', 'p') THEN 0           -- First base types, enums, pseudo-types\n       WHEN typtype = 'r' THEN 1                        -- Ranges after\n       WHEN typtype = 'c' THEN 2                        -- Composites after\n       WHEN typtype = 'd' AND elemtyptype <> 'a' THEN 3 -- Domains over non-arrays after\n       WHEN typtype = 'a' THEN 4                        -- Arrays before\n       WHEN typtype = 'd' AND elemtyptype = 'a' THEN 5  -- Domains over arrays last\n    END AS ord\nFROM (\n    -- Arrays have typtype=b - this subquery identifies them by their typreceive and converts their typtype to a\n    -- We first do this for the type (innerest-most subquery), and then for its element type\n    -- This also returns the array element, range subtype and domain base type as elemtypoid\n    SELECT\n        typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.typrelid, typ.typnotnull, typ.relkind,\n        elemtyp.oid AS elemtypoid, elemtyp.typname AS elemtypname, elemcls.relkind AS elemrelkind,\n        CASE WHEN elemproc.proname='array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype\n    FROM (\n        SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind, typelem AS elemoid,\n            CASE WHEN proc.proname='array_recv' THEN 'a' ELSE typ.typtype END AS typtype,\n            CASE\n                WHEN proc.proname='array_recv' THEN typ.typelem\n                WHEN typ.typtype='r' THEN rngsubtype\n                WHEN typ.typtype='d' THEN typ.typbasetype\n            END AS elemtypoid\n        FROM pg_type AS typ\n        LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid)\n        LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive\n        LEFT JOIN pg_range ON (pg_range.rngtypid = typ.oid)\n    ) AS typ\n    LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid\n    LEFT JOIN pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid)\n    LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive\n) AS typ_and_elem_type\nJOIN pg_namespace AS ns ON (ns.oid = typnamespace)\nWHERE\n    typtype IN ('b', 'r', 'e', 'd') OR -- Base, range, enum, domain\n    (typtype = 'c' AND relkind='c') OR -- User-defined free-standing composites (not table composites) by default\n    (typtype = 'p' AND typname IN ('record', 'void')) OR -- Some special supported pseudo-types\n    (typtype = 'a' AND (  -- Array of...\n        elemtyptype IN ('b', 'r', 'e', 'd') OR -- Array of base, range, enum, domain\n        (elemtyptype = 'p' AND elemtypname IN ('record', 'void')) OR -- Arrays of special supported pseudo-types\n        (elemtyptype = 'c' AND elemrelkind='c') -- Array of user-defined free-standing composites (not table composites) by default\n    ))\nORDER BY ord";
        public static readonly PgTable Npgsql4TypesResponse = CsvToPg.Convert(
            @"npgsql_types_4.csv",
            new Dictionary<string, PgColumn>
            {
                {"nspname", new PgColumn("nspname", 0, PgName.Default, PgFormat.Text)},
                {"oid", new PgColumn("oid", 1, PgOid.Default, PgFormat.Text)},
                {"typnamespace", new PgColumn("typnamespace", 2, PgOid.Default, PgFormat.Text)},
                {"typname", new PgColumn("typname", 3, PgName.Default, PgFormat.Text)},
                {"typtype", new PgColumn("typtype", 4, PgChar.Default, PgFormat.Text, 1)},
                {"typrelid", new PgColumn("typrelid", 5, PgOid.Default, PgFormat.Text)},
                {"typnotnull", new PgColumn("typnotnull", 6, PgBool.Default, PgFormat.Text)},
                {"relkind", new PgColumn("relkind", 7, PgChar.Default, PgFormat.Text, 1)},
                {"elemtypoid", new PgColumn("elemtypoid", 8, PgOid.Default, PgFormat.Text)},
                {"elemtypname", new PgColumn("elemtypname", 9, PgName.Default, PgFormat.Text)},
                {"elemrelkind", new PgColumn("elemrelkind", 10, PgChar.Default, PgFormat.Text, 1)},
                {"elemtyptype", new PgColumn("elemtyptype", 11, PgChar.Default, PgFormat.Text, 1)},
                {"ord", new PgColumn("ord", 12, PgInt4.Default, PgFormat.Text)},
            });

        // 4.1.0 - 4.1.2
        public static readonly string Npgsql4_1_2TypesQuery = "\n/*** Load all supported types ***/\nSELECT ns.nspname, a.typname, a.oid, a.typbasetype,\nCASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS typtype,\nCASE\n  WHEN pg_proc.proname='array_recv' THEN a.typelem\n  WHEN a.typtype='r' THEN rngsubtype\n  ELSE 0\nEND AS typelem,\nCASE\n  WHEN a.typtype='d' AND a.typcategory='A' THEN 4 /* Domains over arrays last */\n  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3    /* Arrays before */\n  WHEN a.typtype='r' THEN 2                                        /* Ranges before */\n  WHEN a.typtype='d' THEN 1                                        /* Domains before */\n  ELSE 0                                                           /* Base types first */\nEND AS ord\nFROM pg_type AS a\nJOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)\nJOIN pg_proc ON pg_proc.oid = a.typreceive\nLEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)\nLEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)\nLEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)\nLEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid) \nWHERE\n  a.typtype IN ('b', 'r', 'e', 'd') OR         /* Base, range, enum, domain */\n  (a.typtype = 'c' AND cls.relkind='c') OR /* User-defined free-standing composites (not table composites) by default */\n  (pg_proc.proname='array_recv' AND (\n    b.typtype IN ('b', 'r', 'e', 'd') OR       /* Array of base, range, enum, domain */\n    (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR /* Arrays of special supported pseudo-types */\n    (b.typtype = 'c' AND elemcls.relkind='c')  /* Array of user-defined free-standing composites (not table composites) */\n  )) OR\n  (a.typtype = 'p' AND a.typname IN ('record', 'void'))  /* Some special supported pseudo-types */\nORDER BY ord";
        public static readonly PgTable Npgsql4_1_2TypesResponse = CsvToPg.Convert(
            @"npgsql_types_4_1_2.csv",
            new Dictionary<string, PgColumn>
            {
                {"nspname", new PgColumn("nspname", 0, PgName.Default, PgFormat.Text)},
                {"typname", new PgColumn("typname", 1, PgName.Default, PgFormat.Text)},
                {"oid", new PgColumn("oid", 2, PgOid.Default, PgFormat.Text)},
                {"typbasetype", new PgColumn("typbasetype", 3, PgOid.Default, PgFormat.Text)},
                {"typtype", new PgColumn("typtype", 4, PgChar.Default, PgFormat.Text, 1)},
                {"typelem", new PgColumn("typelem", 5, PgOid.Default, PgFormat.Text)},
                {"ord", new PgColumn("ord", 6, PgInt4.Default, PgFormat.Text)},
            });

        // 4.0.3
        public static readonly string Npgsql4_0_3TypesQuery = "/*** Load all supported types ***/\nSELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,\nCASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,\nCASE\n  WHEN pg_proc.proname='array_recv' THEN a.typelem\n  WHEN a.typtype='r' THEN rngsubtype\n  ELSE 0\nEND AS elemoid,\nCASE\n  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3    /* Arrays last */\n  WHEN a.typtype='r' THEN 2                                        /* Ranges before */\n  WHEN a.typtype='d' THEN 1                                        /* Domains before */\n  ELSE 0                                                           /* Base types first */\nEND AS ord\nFROM pg_type AS a\nJOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)\nJOIN pg_proc ON pg_proc.oid = a.typreceive\nLEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)\nLEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)\nLEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)\nLEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid) \nWHERE\n  a.typtype IN ('b', 'r', 'e', 'd') OR         /* Base, range, enum, domain */\n  (a.typtype = 'c' AND cls.relkind='c') OR /* User-defined free-standing composites (not table composites) by default */\n  (pg_proc.proname='array_recv' AND (\n    b.typtype IN ('b', 'r', 'e', 'd') OR       /* Array of base, range, enum, domain */\n    (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR /* Arrays of special supported pseudo-types */\n    (b.typtype = 'c' AND elemcls.relkind='c')  /* Array of user-defined free-standing composites (not table composites) */\n  )) OR\n  (a.typtype = 'p' AND a.typname IN ('record', 'void'))  /* Some special supported pseudo-types */\nORDER BY ord";
        public static readonly PgTable Npgsql4_0_3TypesResponse = CsvToPg.Convert(
            @"npgsql_types_4_0_3.csv",
            new Dictionary<string, PgColumn>
            {
                {"nspname", new PgColumn("nspname", 0, PgName.Default, PgFormat.Text)},
                {"typname", new PgColumn("typname", 1, PgName.Default, PgFormat.Text)},
                {"oid", new PgColumn("oid", 2, PgOid.Default, PgFormat.Text)},
                {"typrelid", new PgColumn("typrelid", 3, PgOid.Default, PgFormat.Text)},
                {"typbasetype", new PgColumn("typbasetype", 4, PgOid.Default, PgFormat.Text)},
                {"type", new PgColumn("type", 5, PgChar.Default, PgFormat.Text, 1)},
                {"elemoid", new PgColumn("elemoid", 6, PgOid.Default, PgFormat.Text)},
                {"ord", new PgColumn("ord", 7, PgInt4.Default, PgFormat.Text)},
            });

        // 4.0.1 - 4.0.12 (except for 4.0.3)
        public static string TypesQuery = "\n/*** Load all supported types ***/\nSELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,\nCASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,\nCASE\n  WHEN pg_proc.proname='array_recv' THEN a.typelem\n  WHEN a.typtype='r' THEN rngsubtype\n  ELSE 0\nEND AS elemoid,\nCASE\n  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3    /* Arrays last */\n  WHEN a.typtype='r' THEN 2                                        /* Ranges before */\n  WHEN a.typtype='d' THEN 1                                        /* Domains before */\n  ELSE 0                                                           /* Base types first */\nEND AS ord\nFROM pg_type AS a\nJOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)\nJOIN pg_proc ON pg_proc.oid = a.typreceive\nLEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)\nLEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)\nLEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)\nLEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid) \nWHERE\n  a.typtype IN ('b', 'r', 'e', 'd') OR         /* Base, range, enum, domain */\n  (a.typtype = 'c' AND cls.relkind='c') OR /* User-defined free-standing composites (not table composites) by default */\n  (pg_proc.proname='array_recv' AND (\n    b.typtype IN ('b', 'r', 'e', 'd') OR       /* Array of base, range, enum, domain */\n    (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR /* Arrays of special supported pseudo-types */\n    (b.typtype = 'c' AND elemcls.relkind='c')  /* Array of user-defined free-standing composites (not table composites) */\n  )) OR\n  (a.typtype = 'p' AND a.typname IN ('record', 'void'))  /* Some special supported pseudo-types */\nORDER BY ord";

        public static readonly PgTable TypesResponse = CsvToPg.Convert(
            @"types_query.csv",
            new Dictionary<string, PgColumn>
            {
                { "nspname", new PgColumn("nspname", 0, PgName.Default, PgFormat.Text) },
                { "typname", new PgColumn("typname", 1, PgName.Default, PgFormat.Text) },
                { "oid", new PgColumn("oid", 2, PgOid.Default, PgFormat.Text) },
                { "typrelid", new PgColumn("typrelid", 3, PgOid.Default, PgFormat.Text) },
                { "typbasetype", new PgColumn("typbasetype", 4, PgOid.Default, PgFormat.Text) },
                { "type", new PgColumn("type", 5, PgChar.Default, PgFormat.Text, 1) },
                { "elemoid", new PgColumn("elemoid", 6, PgOid.Default, PgFormat.Text) },
                { "ord", new PgColumn("ord", 7, PgInt4.Default, PgFormat.Text) },
            });

        // 4.0.0
        public static readonly string Npgsql4_0_0TypesQuery = "\n/*** Load all supported types ***/\nSELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,\nCASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,\nCASE\n  WHEN pg_proc.proname='array_recv' THEN a.typelem\n  WHEN a.typtype='r' THEN rngsubtype\n  ELSE 0\nEND AS elemoid,\nCASE\n  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3    /* Arrays last */\n  WHEN a.typtype='r' THEN 2                                        /* Ranges before */\n  WHEN a.typtype='d' THEN 1                                        /* Domains before */\n  ELSE 0                                                           /* Base types first */\nEND AS ord\nFROM pg_type AS a\nJOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)\nJOIN pg_proc ON pg_proc.oid = a.typreceive\nLEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)\nLEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)\nLEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)\nLEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid) \nWHERE\n  a.typtype IN ('b', 'r', 'e', 'd') OR         /* Base, range, enum, domain */\n  (a.typtype = 'c' AND cls.relkind='c') OR /* User-defined free-standing composites (not table composites) by default */\n  (pg_proc.proname='array_recv' AND (\n    b.typtype IN ('b', 'r', 'e', 'd') OR       /* Array of base, range, enum domain */\n    (b.typtype = 'c' AND elemcls.relkind='c')  /* Array of user-defined free-standing composites (not table composites) */\n  )) OR\n  (a.typtype = 'p' AND a.typname IN ('record', 'void'))  /* Some special supported pseudo-types */\nORDER BY ord";
        public static readonly PgTable Npgsql4_0_0TypesResponse = CsvToPg.Convert(
            @"npgsql_types_4_0_0.csv",
            new Dictionary<string, PgColumn>
            {
                {"nspname", new PgColumn("nspname", 0, PgName.Default, PgFormat.Text)},
                {"typname", new PgColumn("typname", 1, PgName.Default, PgFormat.Text)},
                {"oid", new PgColumn("oid", 2, PgOid.Default, PgFormat.Text)},
                {"typrelid", new PgColumn("typrelid", 3, PgOid.Default, PgFormat.Text)},
                {"typbasetype", new PgColumn("typbasetype", 4, PgOid.Default, PgFormat.Text)},
                {"type", new PgColumn("type", 5, PgChar.Default, PgFormat.Text, 1)},
                {"elemoid", new PgColumn("elemoid", 6, PgOid.Default, PgFormat.Text)},
                {"ord", new PgColumn("ord", 7, PgInt4.Default, PgFormat.Text)},
            });

        // 4.0.0 - 4.0.3
        public static readonly string Npgsql4_0_0CompositeTypesQuery = "/*** Load field definitions for (free-standing) composite types ***/\nSELECT typ.oid, att.attname, att.atttypid\nFROM pg_type AS typ\nJOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)\nJOIN pg_class AS cls ON (cls.oid = typ.typrelid)\nJOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)\nWHERE\n  (typ.typtype = 'c' AND cls.relkind='c') AND\n  attnum > 0 AND     /* Don't load system attributes */\n  NOT attisdropped\nORDER BY typ.typname, att.attnum";
        public static readonly PgTable Npgsql4_0_0CompositeTypesResponse = new()
        {
            Columns = new List<PgColumn>
            {
                new PgColumn("oid", 0, PgOid.Default, PgFormat.Text),
                new PgColumn("attname", 1, PgName.Default, PgFormat.Text),
                new PgColumn("atttypid", 2, PgOid.Default, PgFormat.Text),
            }
        };

        // 3.2.3 - 3.2.7 (anything below 3.2.3 is generally not supported)
        public static readonly string Npgsql3TypesQuery = "SELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,\nCASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,\nCASE\n  WHEN pg_proc.proname='array_recv' THEN a.typelem\n  WHEN a.typtype='r' THEN rngsubtype\n  ELSE 0\nEND AS elemoid,\nCASE\n  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3    /* Arrays last */\n  WHEN a.typtype='r' THEN 2                                        /* Ranges before */\n  WHEN a.typtype='d' THEN 1                                        /* Domains before */\n  ELSE 0                                                           /* Base types first */\nEND AS ord\nFROM pg_type AS a\nJOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)\nJOIN pg_proc ON pg_proc.oid = a.typreceive\nLEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)\nLEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid) \nWHERE\n  (\n    a.typtype IN ('b', 'r', 'e', 'd') AND\n    (b.typtype IS NULL OR b.typtype IN ('b', 'r', 'e', 'd'))  /* Either non-array or array of supported element type */\n  ) OR\n  (a.typname IN ('record', 'void') AND a.typtype = 'p')\nORDER BY ord";
        public static readonly PgTable Npgsql3TypesResponse = CsvToPg.Convert(
            @"npgsql_types_3.csv",
            new Dictionary<string, PgColumn>
            {
                {"nspname", new PgColumn("nspname", 0, PgName.Default, PgFormat.Text)},
                {"typname", new PgColumn("typname", 1, PgName.Default, PgFormat.Text)},
                {"oid", new PgColumn("oid", 2, PgOid.Default, PgFormat.Text)},
                {"typrelid", new PgColumn("typrelid", 3, PgOid.Default, PgFormat.Text)},
                {"typbasetype", new PgColumn("typbasetype", 4, PgOid.Default, PgFormat.Text)},
                {"type", new PgColumn("type", 5, PgChar.Default, PgFormat.Text, 1)},
                {"elemoid", new PgColumn("elemoid", 6, PgOid.Default, PgFormat.Text)},
                {"ord", new PgColumn("ord", 7, PgInt4.Default, PgFormat.Text)},
            });
    }
}
