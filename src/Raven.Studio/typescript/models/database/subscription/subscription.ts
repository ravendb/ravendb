/// <reference path="../../../../typings/tsd.d.ts"/>

class Subscription {

    ackEtag = ko.observable<string>();
    newAckEtag = ko.observable<string>();
    subscriptionId: number;
    isChangeInProgress = ko.observable<boolean>();
    isButtonDisabled: KnockoutComputed<boolean>;
    etagCustomValidityError: KnockoutComputed<string>;

    constructor(private id: number, private etag: string) {
        this.subscriptionId = id;
        this.ackEtag(etag);
        this.newAckEtag(etag);

        this.isButtonDisabled = ko.computed(() => this.ackEtag() === this.newAckEtag() || this.isChangeInProgress());

        this.etagCustomValidityError = ko.computed(() => {
            var errorMessage: string = "";
            var currentNewAckEtag = this.newAckEtag();
            if (currentNewAckEtag.length !== 36) {
                errorMessage = "Etag length is invalid";
            }
            else if (currentNewAckEtag[8] !== "-" || currentNewAckEtag[13] !== "-" ||
                currentNewAckEtag[18] !== "-" || currentNewAckEtag[23] !== "-") {
                errorMessage = "Invalid etag format";
            }

            return errorMessage;
        });
    }
}

export = Subscription;