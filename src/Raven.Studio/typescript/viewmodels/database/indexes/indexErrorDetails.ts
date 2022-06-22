import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import popoverUtils = require("common/popoverUtils");

class indexErrorDetails extends dialogViewModelBase {

    view = require("views/database/indexes/indexErrorDetails.html");

    private indexErrors: Array<IndexErrorPerDocument>;
    private currentIndex = ko.observable<number>();

    private currentError: KnockoutComputed<IndexErrorPerDocument>;
    private canNavigateToPreviousError: KnockoutComputed<boolean>;
    private canNavigateToNextError: KnockoutComputed<boolean>;

    constructor(indexErrors: Array<IndexErrorPerDocument>, initialIndex: number) {
        super();

        this.indexErrors = indexErrors;
        this.currentIndex(initialIndex);

        this.initObservables();
    }

    compositionComplete() {
        super.compositionComplete();
        
        popoverUtils.longWithHover($(".js-time-value"),
            {
                content: `<div class="data-container padding padding-xs">
                              <div>
                                  <div class="data-label">UTC:</div>
                                  <div class="data-value">${this.currentError().Timestamp}</div>
                              </div>
                              <div>
                                  <div class="data-label">Relative:</div>
                                  <div class="data-value">${this.currentError().RelativeTime}</div>
                              </div>
                          </div>`
            });
    }

    private initObservables() {
        this.currentError = ko.pureComputed(() => this.indexErrors[this.currentIndex()]);
        this.canNavigateToPreviousError = ko.pureComputed(() => this.currentIndex() > 0);
        this.canNavigateToNextError = ko.pureComputed(() => this.currentIndex() < this.indexErrors.length - 1);
    }

    previousError() {
        const idx = this.currentIndex();
        if (this.canNavigateToPreviousError()) {
            this.currentIndex(idx - 1);
        }
    }

    nextError() {
        const idx = this.currentIndex();
        if (this.canNavigateToNextError()) {
            this.currentIndex(idx + 1);
        }
    }
}

export = indexErrorDetails; 
