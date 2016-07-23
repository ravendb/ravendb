/// <reference path="../../typings/tsd.d.ts"/>

class layoutSwitcher {

    newLayoutMode = ko.observable<boolean>(false);

    constructor() {
        this.newLayoutMode.subscribe(newMode => {
            if (newMode) {
                let oldSelection = document.querySelectorAll("link[data-layout=old]");
                for (let i = 0; i < oldSelection.length; i++) {
                    let style = oldSelection[i];
                    style.setAttribute('disabled', "true");
                }

                let newSelection = document.querySelectorAll("link[data-layout=new]");
                for (let i = 0; i < newSelection.length; i++) {
                    let style = newSelection[i];
                    style.removeAttribute('disabled');
                }
            } else {
                let oldSelection = document.querySelectorAll("link[data-layout=old]");
                for (let i = 0; i < oldSelection.length; i++) {
                    let style = oldSelection[i];
                    style.removeAttribute('disabled');
                }

                let newSelection = document.querySelectorAll("link[data-layout=new]");
                for (let i = 0; i < newSelection.length; i++) {
                    let style = newSelection[i];
                    style.setAttribute('disabled', "true");
                }
            }
        });
    }

    setMode(newMode: boolean) {
        this.newLayoutMode(newMode);
    }
}

export = layoutSwitcher;
