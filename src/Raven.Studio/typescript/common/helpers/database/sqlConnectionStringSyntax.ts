

class sqlConnectionStringSyntax {
    
    private constructor() {
        // static class
    }

    static readonly mssqlSyntax = `Example: <code>Data Source=10.0.0.107;Database=SourceDB;User ID=sa;Password=secret;</code>
                <br />
                <small>
                    More examples can be found in 
                    <a href="https://ravendb.net/l/38S9OQ" target="_blank"><i class="icon-link"></i> full syntax reference</a>
                </small>`;
    
    static readonly mysqlSyntax = `Example: <code>server=10.0.0.103;port=3306;userid=root;password=secret;</code>
                <br />
                <small>
                    More examples can be found in 
                    <a href="https://ravendb.net/l/BSS8YH" target="_blank"><i class="icon-link"></i> full syntax reference</a>
                </small>`;
    
    static readonly npgsqlSyntax = `Example: <code>Host=10.0.0.105;Port=5432;Username=postgres;Password=secret</code>
                <br />
                <small>
                    More examples can be found in 
                    <a href="https://ravendb.net/l/FWEBWD" target="_blank"><i class="icon-link"></i> full syntax reference</a>
                </small>`;
    
    static readonly oracleSyntax = `Example: <code>Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.0.101)(PORT=1521)))(CONNECT_DATA=(SID=ORCLCDB)));User Id=SYS;DBA Privilege=SYSDBA;password=secret;</code>
                <br />
                <small>
                    More examples can be found in 
                    <a href="https://ravendb.net/l/TG851N" target="_blank"><i class="icon-link"></i> full syntax reference</a>
                </small>`;
}

export = sqlConnectionStringSyntax;
