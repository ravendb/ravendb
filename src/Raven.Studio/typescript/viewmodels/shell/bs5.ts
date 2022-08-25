import viewModelBase = require("viewmodels/viewModelBase");
import { BootstrapPlaygroundPage } from "../../components/pages/BootstrapPlaygroundPage";

class bs5 extends viewModelBase {
    view = { default: `<div class="content-margin" data-bind="react: reactOptions"></div>` };

    isUsingBootstrap5(): boolean | undefined {
        return true;
    }

    reactOptions = ko.pureComputed(() => {
        return ({
            component: BootstrapPlaygroundPage,
        });
    });
}


export = bs5;
