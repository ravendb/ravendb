%output=SqlParser.cs
%namespace Microsoft.Isam.Esent.Sql.Parsing
%using Microsoft.Isam.Esent
%partial

%union
{ 
	internal string name;
	internal string stringValue;
	internal long intValue;
	internal double realValue;
	internal object value;

	internal ColumnType coltyp; 	
	internal ColumnDefinition columndef;
	internal List<ColumnDefinition> columndefs;
	
	internal List<string> columnNames;
	internal List<object> columnValues;
}

%token NAME
%token STRING
%token INTEGER
%token REAL_NUMBER

%left OR
%left AND
%left NOT
%left COMPARISON
%left '+' '-'
%left '*' '/'
%nonassoc UMINUS

%token ATTACH CREATE DATABASE DETACH INDEX TABLE 
%token BEGIN COMMIT END TRANSACTION ROLLBACK TO SAVEPOINT RELEASE
%token BOOL BYTE SHORT INT LONG TEXT BINARY DATETIME GUID 
%token INSERT INTO VALUES

%%
		
sql:			createdb
		|		attachdb
		|		detachdb
		|		createtable
		|		begin_trx
		|		commit_trx
		|		rollback_trx
		|		create_savepoint
		|		release_savepoint
		|		insert
		| 		blank
		;
		
blank:			EOF
		;
		
createdb:		CREATE DATABASE database { this.SqlImplementation.CreateDatabase($3.stringValue); }
		;
		
attachdb:		ATTACH DATABASE database { this.SqlImplementation.AttachDatabase($3.stringValue); }
		;

detachdb:		DETACH DATABASE { this.SqlImplementation.DetachDatabase(); }
		;
		
createtable: 	CREATE TABLE table '(' columncreate_list ')' { this.SqlImplementation.CreateTable($3.name, $5.columndefs); }
		;
		
columncreate_list:	columncreate
					{
							$$.columndefs = new List<ColumnDefinition>();
							$$.columndefs.Add($1.columndef);
					}
		|			columncreate_list ',' columncreate
					{
							$$.columndefs = new List<ColumnDefinition>();
							$$.columndefs.AddRange($1.columndefs);
							$$.columndefs.Add($3.columndef);
					}
		;
			
columncreate:		column data_type
					{
						$$.columndef = new ColumnDefinition($1.name, $2.coltyp);
					}
		;
		
data_type:		BOOL		{ $$.coltyp = ColumnType.Bool; }
		|		BYTE		{ $$.coltyp = ColumnType.Byte; }
		|		SHORT		{ $$.coltyp = ColumnType.Short; }
		|		INT			{ $$.coltyp = ColumnType.Int; }
		|		LONG		{ $$.coltyp = ColumnType.Long; }
		|		TEXT		{ $$.coltyp = ColumnType.Text; }
		|		BINARY		{ $$.coltyp = ColumnType.Binary; }
		|		DATETIME	{ $$.coltyp = ColumnType.DateTime; }
		|		GUID		{ $$.coltyp = ColumnType.Guid; }
		;

begin_trx:		BEGIN opt_transaction	{ this.SqlImplementation.BeginTransaction(); }
		;

opt_transaction:	
		|		TRANSACTION
		;
		
commit_trx:		COMMIT opt_transaction	{ this.SqlImplementation.CommitTransaction(); }
		|		END opt_transaction		{ this.SqlImplementation.CommitTransaction(); }
		;
		
rollback_trx:	ROLLBACK opt_transaction							{ this.SqlImplementation.RollbackTransaction(); }
		|		ROLLBACK opt_transaction TO opt_savepoint savepoint	{ this.SqlImplementation.RollbackToSavepoint($5.name); }
		;
		
opt_savepoint:
		|		SAVEPOINT
		;
		
create_savepoint:		SAVEPOINT savepoint			{ this.SqlImplementation.CreateSavepoint($2.name); }
		;

release_savepoint:	RELEASE opt_savepoint savepoint	{ this.SqlImplementation.CommitSavepoint($3.name); }
		;
				
insert:				INSERT INTO table '(' columnname_list ')' VALUES '(' value_list ')'
					{
							string tableName = $3.name;
							string[] columnNames = $5.columnNames.ToArray();
							object[] columnValues = $9.columnValues.ToArray();
							
							if (columnNames.Length != columnValues.Length)
							{
								throw new EsentSqlParseException("Different number of column names and column values");
							}
							
							KeyValuePair<string, object>[] dataToSet = new KeyValuePair<string, object>[columnNames.Length];
							for (int i = 0; i < columnNames.Length; ++i)
							{
								dataToSet[i] = new KeyValuePair<string, object>( columnNames[i], columnValues[i] );
							}														
							
							this.SqlImplementation.InsertRecord(tableName, dataToSet);
					}
		;
		
columnname_list:	column
					{
						$$.columnNames = new List<string>();
						$$.columnNames.Add($1.name);
					}
		|			columnname_list ',' column
					{
						$$.columnNames = $1.columnNames;
						$$.columnNames.Add($3.name);
					}
		;			

value_list:			value
					{
						$$.columnValues = new List<object>();
						$$.columnValues.Add($1.value);
					}
		|			value_list ',' value
					{
						$$.columnValues = $1.columnValues;
						$$.columnValues.Add($3.value);
					}
		;			
		
value:			STRING		{ $$.value = $1.stringValue; }
		|		INTEGER		{ $$.value = $1.intValue; }
		|		REAL_NUMBER	{ $$.value = $1.realValue; }
		;
		
table:			NAME
		;
		
column:			NAME
		;
				
database:		STRING
		;
		
savepoint:		NAME
		;

%%		

