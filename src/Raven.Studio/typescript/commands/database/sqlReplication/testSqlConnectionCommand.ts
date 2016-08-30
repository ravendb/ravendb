import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

 class testSqlConnectionCommand extends commandBase{
     constructor(private db: database, private factoryName: string, private connectionString:string) {
         super();
     }

     execute(): JQueryPromise<any> {
         var args = {
             factoryName: this.factoryName,
             connectionString: this.connectionString
         };
         return this.query<any>("/studio-tasks/test-sql-replication-connection", args, this.db, null, 60000);//TODO: use endpoints
     }
 }

 export = testSqlConnectionCommand;
