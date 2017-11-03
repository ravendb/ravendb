import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");
import finishSetupCommand = require("commands/setup/finishSetupCommand");

class finish extends setupStep {

  compositionComplete() {
      super.compositionComplete();
      
      
      new finishSetupCommand()
          .execute()
          .done(() => {
              setTimeout(() => this.redirectToStudio(), 3000);
          });
      
  }
  
  private redirectToStudio() {
      switch (this.model.mode()) {
          case "Unsecured":
              window.location.href = this.model.unsecureSetup().serverUrl();
              break;
      }
      
      //TODO: finish me!
      
  }

}

export = finish;
