import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

 class testSqlConnectionCommand extends commandBase{
     constructor(private db: database, private factoryName: string, private connectionString:string) {
         super();
     }

     execute(): JQueryPromise<any> {
         const args = {
             factoryName: this.factoryName,
             connectionString: this.connectionString
         };
         return this.query<any>(endpoints.databases.sqlReplication.sqlReplicationTestSqlConnection, args, this.db, null, 60000);
     }
 }

 export = testSqlConnectionCommand;
