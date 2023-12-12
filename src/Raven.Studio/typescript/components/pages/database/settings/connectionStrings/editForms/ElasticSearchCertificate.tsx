import certificateUtils from "common/certificateUtils";
import { Icon } from "components/common/Icon";
import replicationCertificateModel from "models/database/tasks/replicationCertificateModel";
import moment from "moment";
import React from "react";
import { Card, CardBody, CardHeader, Col } from "reactstrap";
import IconName from "typings/server/icons";

interface ElasticSearchCertificateProps {
    certBase64: string;
}

export default function ElasticSearchCertificate({ certBase64 }: ElasticSearchCertificateProps) {
    // const publicKey = certificateUtils.extractBase64(certBase64);

    // const certInfo = certificateUtils.extractCertificateInfo(certBase64);

    const x = new replicationCertificateModel(certBase64);

    return <div>{x.thumbprint()}</div>;

    // const expirationMoment = moment.utc(certInfo.expiration);
    // const dateFormatted = expirationMoment.format("YYYY-MM-DD");

    // const expirationText = (expirationMoment.isBefore() && "Expired ") + dateFormatted;
    // const expirationIcon: IconName = expirationMoment.isBefore() ? "danger" : "expiration";
    // const expirationClass = expirationMoment.isBefore() ? "text-danger" : "";

    // const notBeforeMoment = moment.utc(certInfo.notBefore);
    // const validFromText = notBeforeMoment.format("YYYY-MM-DD");

    // return (
    //     <Card className="well">
    //         <CardHeader>
    //             <Icon icon="certificate" />
    //             {certInfo.thumbprint}
    //         </CardHeader>
    //         <CardBody>
    //             <Col>
    //                 <div>
    //                     <Icon icon="clock" />
    //                     Valid From
    //                 </div>
    //                 <div>
    //                     <strong>{validFromText}</strong>
    //                 </div>
    //             </Col>
    //             <Col>
    //                 <div>
    //                     <Icon icon={expirationIcon} />
    //                     Expiration
    //                 </div>
    //                 <div>
    //                     <strong className={expirationClass}>{expirationText}</strong>
    //                 </div>
    //             </Col>
    //         </CardBody>
    //     </Card>
    // );
}
