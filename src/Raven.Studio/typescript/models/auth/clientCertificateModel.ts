/// <reference path="../../../typings/tsd.d.ts" />

import getClientCertificateCommand = require("commands/auth/getClientCertificateCommand");
import moment from "moment";

type clientCertificateExpiration = "unknown" | "valid" | "aboutToExpire" | "expired";
const aboutToExpirePeriod = moment.duration(14, "days");

class clientCertificateModel {
    static certificateInfo = ko.observable<Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition>();
    
    static certificateExpirationState = ko.pureComputed<clientCertificateExpiration>(() => {
        const info = clientCertificateModel.certificateInfo();
        if (!info) {
            return "unknown";
        }

        if (!info.NotAfter) {
            // master key case
            return "valid";
        }
        
        const notAfter = moment(info.NotAfter);
        if (notAfter.isBefore()) {
            return "expired";
        }
        
        const warningDate = moment().add(aboutToExpirePeriod);
        if (notAfter.isBefore(warningDate)) {
            return "aboutToExpire";
        }
        
        return "valid";
    });
    
    static fetchClientCertificate(): JQueryPromise<Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition> {
        return new getClientCertificateCommand()
            .execute()
            .done((result: Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition) => {
                this.certificateInfo(result);
            });
    }
}

export = clientCertificateModel;
