import commandBase = require("commands/commandBase");
import database = require("models/database");

class createSampleDataCommand extends commandBase {

  constructor(private db: database) {
    super();
  }

  execute(): JQueryPromise<any> {
    this.reportInfo("Creating Sample Data, Please wait...");

    return this.post("/studio-tasks/createSampleData", null, this.db, { dataType: 'text' })
      .fail((response)=> this.reportError("Failed to create sample data", response.responseText))
      .done(()=> this.reportSuccess("Sample data creation completed"));
  }

}

export = createSampleDataCommand;